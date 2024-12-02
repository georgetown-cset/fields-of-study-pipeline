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
    norms = np.linalg.norm(vectors, ord=2, axis=1)[:, None]
    return np.divide(vectors, norms, where=norms != 0.0)


def load_meta():
    return pd.read_json(ASSETS_DIR / 'fields/field_meta.jsonl', lines=True)


def batch_fasttext(fasttext, batch):
    vectors = [fasttext.get_sentence_vector(record['text']) for record in batch]
    return row_norm(vectors)


def batch_entities(entities, batch):
    vectors = [embed_entities(record['text'], entities) for record in batch]
    return row_norm(vectors)


def cosine_similarity(docs, fields):
    return np.dot(docs, fields.T)


def batch_tfidf(tfidf, dictionary, batch):
    bow = [dictionary.doc2bow(record['text'].split()) for record in batch]
    dtm = [doc for doc in tfidf.gensim_model[bow]]
    return dtm


def average_similarity(ft, entity, tfidf):
    sims = np.array((ft, tfidf.A, entity))
    valid_mask = (sims >= 0) & (sims <= 1)
    # Originally This occupies about 24% of call time
    valid_sums = np.sum(sims * valid_mask, axis=0)
    valid_counts = np.sum(valid_mask, axis=0)
    avg = np.divide(valid_sums, valid_counts, where=valid_counts > 0)
    return avg


def batch_score(ft, dtm, ent, field_ft, field_tfidf, field_entities):
    ft_sim = cosine_similarity(ft, field_ft)
    tfidf_sim = batch_sparse_similarity(dtm, field_tfidf)
    entity_sim = cosine_similarity(ent, field_entities)
    scores = average_similarity(ft_sim, entity_sim, tfidf_sim)
    return scores


def rank(scores):
    # Fill any NaNs with 0.0 for ranking
    scores = np.nan_to_num(scores, copy=False)
    # Get the indices that would sort the scores ascending and keep the top 10
    ranked_indices = np.argsort(scores, axis=1)[:, -10:]
    # Get the corresponding top 10 scores ascending, matching the ranked_indices
    ranked_scores = scores[np.arange(scores.shape[0])[:, None], ranked_indices]
    return ranked_indices, ranked_scores


def check_constraints(top_l0, top_l1, constraints):
    eligible = []
    constraint_keys = []
    for (l0, l1), l2s in constraints.items():
        if l0 in top_l0 and l1 in top_l1:
            eligible.extend(l2s)
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
    records = []
    for field_id, score in zip(reversed(indices), reversed(scores)):
        if np.isnan(score):
            continue
        records.append({
            'name': index[field_id],
            'score': round(float(score), 4),
        })
    return records

def main(chunk_size=1_000, limit=1_000):
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

    l0l1_levels = levels[levels <= 1]
    l0l1_fasttext = field_fasttext[levels <= 1]
    l0l1_tfidf = field_tfidf[levels <= 1]
    l0l1_entity = field_entities[levels <= 1]

    i = 0
    start_time = timeit.default_timer()
    print(f'[{dt.now().isoformat()}] Starting job')

    with open(CORPUS_DIR / f'en_scores.jsonl', 'wt') as f:
        for batch in chunked(iter_bq_extract('en_'), chunk_size):
            batch_start_time = timeit.default_timer()

            ft = batch_fasttext(model.fasttext, batch)
            dtm = batch_tfidf(model.tfidf, dictionary, batch)
            ent = batch_entities(model.entities, batch)
            scores = batch_score(ft, dtm, ent, l0l1_fasttext, l0l1_tfidf, l0l1_entity)

            top_l0_idx, top_l0_scores = rank(scores[:, l0l1_levels == 0])
            top_l1_idx, top_l1_scores = rank(scores[:, l0l1_levels == 1])

            # Iterate over docs to get what L2/3s they're eligible for given their top L0s and top L1s
            eligible, constraint_keys = zip(*[
                check_constraints(top_l0, top_l1, constraints)
                for (top_l0, top_l1) in zip(top_l0_idx[:, -3:], top_l1_idx[:, -3:])
            ])

            # We'll store L2/3 scores in an N x F array because the indexing is convenient
            l23_scores = np.full((len(batch), len(index)), np.nan)
            for constraint_key, descendants in constraints.items():
                #
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

            l2_indices, l2_scores = rank(l23_scores[:, levels == 2])
            l3_indices, l3_scores = rank(l23_scores[:, levels == 3])

            for j, record in enumerate(batch):
                results = []
                results.extend(to_score_records(top_l0_idx[j], top_l0_scores[j], index))
                results.extend(to_score_records(top_l1_idx[j], top_l1_scores[j], index))
                results.extend(to_score_records(l2_indices[j], l2_scores[j], index))
                results.extend(to_score_records(l3_indices[j], l3_scores[j], index))
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
    parser.add_argument('--batch', type=int, default=10_000, help='Batch size')
    parser.add_argument('--limit', type=int, default=100_000, help='Record limit')
    args = parser.parse_args()
    main(chunk_size=args.batch, limit=args.limit)
