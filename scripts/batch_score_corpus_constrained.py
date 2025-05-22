"""
Score batches of documents in terms of their similarity to fields.

This script achieves some efficiency gains over `batch_score_corpus.py` by restricting
L2/L3 scoring for publications to the L2/L3 fields with a top-3 L0/L1 ancestor. (We
previously imposed this restriction after ingest.) The script also limits output to
top-10 fields in each level.
"""
import argparse
import json
import timeit
from datetime import datetime as dt
from typing import Dict, Tuple, List

import numpy as np
import pandas as pd
from more_itertools import chunked

from fos.entity import embed_entities
from fos.model import FieldModel
from fos.settings import CORPUS_DIR, ASSETS_DIR
from fos.util import iter_bq_extract
from fos.vectors import batch_sparse_similarity


def row_norm(vectors):
    """Normalize document (row) vectors in an array of document embeddings."""
    norms = np.linalg.norm(vectors, ord=2, axis=1)[:, None]
    return np.divide(vectors, norms, where=norms != 0.0)


def load_meta():
    """Load field metadata."""
    return pd.read_json(ASSETS_DIR / 'fields/field_meta.jsonl', lines=True)


def batch_fasttext(fasttext, batch):
    """Embed a batch of texts using FastText."""
    vectors = [fasttext.get_sentence_vector(record['text']) for record in batch]
    return row_norm(vectors)


def batch_entities(entities, batch):
    """Embed a batch of text entities using FastText."""
    vectors = [embed_entities(record['text'], entities) for record in batch]
    return row_norm(vectors)


def cosine_similarity(docs, fields):
    """Calculate the similarity between documents and fields."""
    return np.dot(docs, fields.T)


def batch_tfidf(tfidf, dictionary, batch):
    """Embed a batch of texts using tf-idf."""
    bow = [dictionary.doc2bow(record['text'].split()) for record in batch]
    dtm = [doc for doc in tfidf.gensim_model[bow]]
    return dtm


def average_similarity(ft, entity, tfidf):
    """Average the FastText, entity FastText, and tf-idf similarities for a batch.

    Not all documents will have similarity scores of all types. We take the average
    over available score types.
    """
    sims = np.array((ft, tfidf.A, entity))
    valid_mask = (sims >= 0) & (sims <= 1)
    valid_sums = np.sum(sims * valid_mask, axis=0)
    valid_counts = np.sum(valid_mask, axis=0)
    avg = np.divide(valid_sums, valid_counts, where=valid_counts > 0)
    return avg


def batch_score(ft, dtm, ent, field_ft, field_tfidf, field_entities):
    """Score a batch of document embeddings and return an average score."""
    ft_sim = cosine_similarity(ft, field_ft)
    tfidf_sim = batch_sparse_similarity(dtm, field_tfidf)
    entity_sim = cosine_similarity(ent, field_entities)
    scores = average_similarity(ft_sim, entity_sim, tfidf_sim)
    return scores


def rank(scores, offset=0):
    """Rank the field scores within a level."""
    # Fill any NaNs with 0.0 for ranking
    scores = np.nan_to_num(scores, copy=False)
    # Get the indices that would sort the scores ascending and keep the top 10
    ranked_indices = np.argsort(scores, axis=1)[:, -10:]
    # Get the corresponding top 10 scores ascending, matching the ranked_indices
    ranked_scores = scores[np.arange(scores.shape[0])[:, None], ranked_indices]
    # We passed into this function a slice of the full scores array for ranking within
    # fields, so the indices found here are offset from those in the full scores array
    # by where the field slice begins. To use these indices to reference elements in
    # the full scores array, we need to adjust for that offset. Otherwise, the wrong
    # field names will be associated with scores.
    ranked_indices += offset
    return ranked_indices, ranked_scores


def check_constraints(top_l0, top_l1, constraints):
    """Retrieve eligible L2/3s given top L0s and top L1s."""
    eligible = []
    constraint_keys = []
    for (l0, l1), l23s in constraints.items():
        if l0 in top_l0 and l1 in top_l1:
            eligible.extend(l23s)
            constraint_keys.append((l0, l1))
    return eligible, constraint_keys


def load_constraints() -> Dict[Tuple[int, int], List[int]]:
    """Load constraints as a dict mapping L0 and L1 field indexes to their descendant
     L2 and L3 field indexes.

    There are 12 (L0, L1) field pairs that have L2/L3 descendants. After scoring papers
    for L0/L1s, we only want to score them for L2s and L3s that they're eligible for. We
    require a top three L0 score and a top three L1 score for a paper to be eligible for
    the L2/L3 descendants of that L0 and L1.
    """
    # meta gives us the levels for each field, which don't appear in the children table
    meta = load_meta()
    # The children table gives us all parent-child pairs, so we start by selecting the
    #  L0-L1 pairs.
    children = pd.read_json(ASSETS_DIR / 'fields/field_children.jsonl', lines=True)
    l0_l1 = children.loc[children['parent_name'].isin(meta.loc[meta.level == 0, 'name'])]. \
        rename(columns={'parent_name': 'l0', 'child_name': 'l1'})
    # Inner joining the L0-L1 pairs with the children table then gives us each L0,L1,L2/3 triple,
    # because the L3s are descendants of L1s, and not children of specific L2s. (They're clearly
    # lower-level than L2s, but placing them under particular L2s wasn't possible.)
    constraints = pd.merge(l0_l1, children.rename(columns={'parent_name': 'l1'}), on='l1')
    # Aggregating over L2/L3s we get a list of the L2s/L3s for each L0-L1 pair
    constraints = constraints.groupby(['l0', 'l1'], as_index=False).agg(child_name=('child_name', list))

    # For slicing field embedding matrices we need indexes, not field names
    def to_indices(names):
        return [meta['name'][meta['name'] == name].index[0] for name in names]

    # Map the field names to indices
    constraints['child_idx'] = constraints['child_name'].apply(to_indices)
    constraints['l0_idx'] = to_indices(constraints['l0'])
    constraints['l1_idx'] = to_indices(constraints['l1'])

    # Return a mapping that looks like (8, 1) => [755, 756, 757]
    constraints.set_index(['l0_idx', 'l1_idx'], inplace=True)
    return constraints['child_idx'].to_dict()


def to_score_records(indices, scores, index):
    """Format field scores for a document for BigQuery ingest as an array of structs."""
    records = []
    # The indices and scores are sorted ascending
    for field_id, score in zip(reversed(indices), reversed(scores)):
        if np.isnan(score) or score == 0.0:
            continue
        records.append({
            'name': index[field_id],
            'score': round(float(score), 4),
        })
    return records


def check_distinct(results):
    """Check that field names are distinct within (name, score) results for a paper."""
    names = [field['name'] for field in results]
    if len(names) != len(set(names)):
        raise ValueError('Duplicate field names within the scores for a record')


def main(chunk_size=100_000, limit=100_000, output_path=CORPUS_DIR / "en_scores.jsonl"):
    print(f'[{dt.now().isoformat()}] Loading assets')

    # Load vectors for fields + models for embedding publications
    model = FieldModel()

    # Pulling these arrays out of the model instance is slightly faster
    field_fasttext = model.field_fasttext.index
    field_tfidf = model.field_tfidf.index
    dictionary = model.dictionary
    field_entities = model.field_entities.index

    # Load constraints for scoring L2/L3 fields
    constraints = load_constraints()

    # Field meta
    meta = pd.read_json(ASSETS_DIR / 'fields/field_meta.jsonl', lines=True)
    index = meta['name'].to_numpy()
    levels = meta['level'].to_numpy()

    # If levels isn't monotonic non-decreasing, the offset logic in rank() will fail
    assert np.all(np.diff(levels) >= 0)
    l1_offset = np.argmax(levels == 1).astype(int)
    l2_offset = np.argmax(levels == 2).astype(int)
    l3_offset = np.argmax(levels == 3).astype(int)
    assert 0 < l1_offset < l2_offset  < l3_offset

    # We use the L0-L1 slices of all the assets repeatedly on each batch, so copy them out
    l0l1_levels = levels[levels <= 1]
    l0l1_fasttext = field_fasttext[levels <= 1]
    l0l1_tfidf = field_tfidf[levels <= 1]
    l0l1_entity = field_entities[levels <= 1]

    i = 0
    start_time = timeit.default_timer()
    print(f'[{dt.now().isoformat()}] Starting job')

    with open(output_path, 'wt') as f:
        for batch in chunked(iter_bq_extract('en_'), chunk_size):
            batch_start_time = timeit.default_timer()

            ft = batch_fasttext(model.fasttext, batch)
            dtm = batch_tfidf(model.tfidf, dictionary, batch)
            ent = batch_entities(model.entities, batch)
            scores = batch_score(ft, dtm, ent, l0l1_fasttext, l0l1_tfidf, l0l1_entity)

            top_l0_idx, top_l0_scores = rank(scores[:, l0l1_levels == 0])
            top_l1_idx, top_l1_scores = rank(scores[:, l0l1_levels == 1], l1_offset)

            # Iterate over docs to get what L2/3s they're eligible for given their
            # top L0s and top L1s. The top_l{0,1}_idx arrays are sorted ascending, so to
            # get the top 3 fields in each level by score, we slice into them with -3:
            eligible, constraint_keys = zip(*[
                check_constraints(top_l0, top_l1, constraints)
                for (top_l0, top_l1) in zip(top_l0_idx[:, -3:], top_l1_idx[:, -3:])
            ])

            # We'll store L2/3 scores in an N x F array because the indexing is convenient
            l23_scores = np.full((len(batch), len(index)), np.nan)
            for constraint_key, descendants in constraints.items():
                eligible_mask = np.array([constraint_key in row_keys for row_keys in constraint_keys])
                if not any(eligible_mask):
                    continue
                descendant_scores = batch_score(
                    ft[eligible_mask],
                    [row for (row, mask) in zip(dtm, eligible_mask) if mask],
                    ent[eligible_mask],
                    field_fasttext[descendants],
                    field_tfidf[descendants],
                    field_entities[descendants],
                )
                row_indices = np.where(eligible_mask == True)[0]
                l23_scores[np.ix_(row_indices, np.array(descendants))] = descendant_scores

            l2_indices, l2_scores = rank(l23_scores[:, levels == 2], l2_offset)
            l3_indices, l3_scores = rank(l23_scores[:, levels == 3], l3_offset)

            for j, record in enumerate(batch):
                results = []
                results.extend(to_score_records(top_l0_idx[j], top_l0_scores[j], index))
                results.extend(to_score_records(top_l1_idx[j], top_l1_scores[j], index))
                results.extend(to_score_records(l2_indices[j], l2_scores[j], index))
                results.extend(to_score_records(l3_indices[j], l3_scores[j], index))
                check_distinct(results)
                scores = {
                    'merged_id': record['merged_id'],
                    'fields': results,
                }
                f.write(json.dumps(scores) + '\n')

            i += len(batch)

            batch_stop_time = timeit.default_timer()
            batch_elapsed = round(batch_stop_time - batch_start_time, 1)
            print(f'[{dt.now().isoformat()}] Scored {len(batch):,} docs in {batch_elapsed}s ({i:,} scored so far)')

            if limit and (i >= limit):
                print(f'[{dt.now().isoformat()}] Stopping (--limit was {limit:,})')
                break

    stop_time = timeit.default_timer()
    elapsed = round(stop_time - start_time, 1)
    print(f'[{dt.now().isoformat()}] Scored {i:,} docs in {elapsed}s')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Score merged corpus text')
    parser.add_argument('--batch', type=int, default=100_000, help='Batch size')
    parser.add_argument('--limit', type=int, default=100_000, help='Record limit')
    parser.add_argument('--output', type=str, default=CORPUS_DIR / f'en_scores.jsonl', help='Output path')
    args = parser.parse_args()
    main(chunk_size=args.batch, limit=args.limit, output_path=args.output)
