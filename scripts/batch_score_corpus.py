import argparse
import json
import math
import timeit
from itertools import zip_longest
from datetime import datetime as dt

import numpy as np
from more_itertools import chunked

from fos.entity import load_entities, embed_entities
from fos.settings import CORPUS_DIR
from fos.util import iter_bq_extract
from fos.vectors import load_fasttext, load_tfidf, load_field_fasttext, load_field_tfidf, load_field_entities, \
    load_field_keys, batch_sparse_similarity


def row_norm(vectors):
    norms = np.linalg.norm(vectors, ord=2, axis=1)[:, None]
    return np.divide(vectors, norms, where=norms != 0.0)


def main(lang='en', chunk_size=100_000, limit=100_000):
    print(f'[{dt.now().isoformat()}] Loading assets')
    # Vectors for embedding publications
    fasttext = load_fasttext(lang)
    tfidf, dictionary = load_tfidf(lang)
    entities = load_entities(lang)

    # Field embeddings
    field_fasttext = load_field_fasttext(lang)
    field_tfidf = load_field_tfidf(lang)
    field_entities = load_field_entities(lang)

    # Field embedding index (gives the field IDs corresponding with field score vector elements)
    index = load_field_keys(lang)

    i = 0
    start_time = timeit.default_timer()
    print(f'[{dt.now().isoformat()}] Starting job')

    with open(CORPUS_DIR / f'{lang}_scores.jsonl', 'wt') as f:
        # Break iterable into sub-iterables with chunk_size elements. The last sub-iterable will (probably) have length
        # less than chunk_size.
        for batch in chunked(iter_bq_extract(f'{lang}_'), chunk_size):
            batch_start_time = timeit.default_timer()
            ft = [fasttext.get_sentence_vector(record['text']) for record in batch]
            ft = row_norm(ft)
            ft_sim = np.dot(field_fasttext.index, ft.T).T

            bow = [dictionary.doc2bow(record['text'].split()) for record in batch]
            dtm = [doc for doc in tfidf.gensim_model[bow]]
            tfidf_sim = batch_sparse_similarity(dtm, field_tfidf.index)

            ent = [embed_entities(record['text'], entities) for record in batch]
            ent = row_norm(ent)
            entity_sim = np.dot(field_entities.index, ent.T).T

            sims = np.array((ft_sim, tfidf_sim.A, entity_sim))
            avg_sim = np.apply_along_axis(lambda x: np.average(x[x > 0.0], axis=0), 0, sims)

            for record, row in zip_longest(batch, avg_sim):
                f.write(json.dumps({
                    'merged_id': record['merged_id'],
                    'fields': [
                        {
                            'id': k,
                            'score': None if math.isnan(float(v)) else float(v)
                        }
                        for k, v in zip_longest(index, row)]
                }) + '\n')
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
    parser.add_argument('lang', choices=('en',), help='Language')
    parser.add_argument('--limit', type=int, default=100_000, help='Record limit')
    args = parser.parse_args()
    main(lang=args.lang, limit=args.limit)
