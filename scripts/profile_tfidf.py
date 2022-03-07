import timeit

from fos.model import FieldModel  # noqa
from fos.util import iter_bq_extract
from fos.vectors import load_tfidf, load_field_tfidf, embed_tfidf, sparse_similarity


def embed(lang='en', limit=10_000):
    i = 0
    transformer, dictionary = load_tfidf(lang)
    field_tfidf = load_field_tfidf(lang)
    start_time = timeit.default_timer()
    with open('stream.json', 'wt') as f:
        for record in iter_bq_extract(f'{lang}_'):
            embedding = embed_tfidf(record['text'].split(), transformer, dictionary)
            sim = sparse_similarity(embedding, field_tfidf.index)
            # f.write(json.dumps({'merged_id': record['merged_id'],
            #                     'fields': [{'id': k, 'score': v} for k, v in avg_sim_values]}) + '\n')
            i += 1
            if i == limit:
                break
    stop_time = timeit.default_timer()
    elapsed = round(stop_time - start_time, 1)
    print(f'{limit} docs in {elapsed}s')


if __name__ == '__main__':
    embed("en", 10_000)
