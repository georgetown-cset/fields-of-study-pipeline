import json
import timeit
from itertools import zip_longest

import numpy as np
from more_itertools import grouper

from fos.entity import load_entities, embed_entities
from fos.util import iter_bq_extract
from fos.vectors import load_fasttext, load_tfidf, load_field_fasttext, load_field_tfidf, load_field_entities, \
    load_field_keys, batch_sparse_similarity


def row_norm(vectors):
    norms = np.linalg.norm(vectors, ord=2, axis=1)[:, None]
    return np.divide(vectors, norms, where=norms != 0.0)


def main(lang='en', chunk_size=1_000, limit=1_000):
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

    with open('batch.json', 'wt') as f:
        for batch in grouper(iter_bq_extract(f'{lang}_'), chunk_size):
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
                    'fields': [{'id': k, 'score': float(v)} for k, v in zip_longest(index, row)]
                }) + '\n')

            i += len(batch)
            if i >= limit:
                break

    stop_time = timeit.default_timer()
    elapsed = round(stop_time - start_time, 1)
    print(f'{limit} docs in {elapsed}s')


if __name__ == '__main__':
    main()
