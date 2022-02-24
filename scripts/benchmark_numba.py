import argparse
import json
import timeit
from itertools import zip_longest

from fos.model import FieldModel  # noqa
from fos.util import iter_bq_extract


def time(lang='en'):
    print(timeit.timeit('embed(fields)', setup=f'fields = FieldModel("{lang}")', globals=globals(), number=10))


def score(fields, lang='en', limit=1000):
    i = 0
    start_time = timeit.default_timer()
    with open('stream.json', 'wt') as f:
        # There's JSON parsing here that could be a bit faster, but it's a tiny % of runtime
        for record in iter_bq_extract(f'{lang}_'):
            # Embedding via the embed method calls embed_fasttext(), embed_tfidf(), and embed_entities().
            # The greatest overhead is in embed_tfidf. There's also some room for improvement in embed_entities.
            embedding = fields.embed(record['text'])
            # Scoring is far and away the most expensive operation. For each embedding method (FT, tfidf, entity FT),
            # we have ~300 field vectors and a document vector. We want to calculate the cosine similarity of a ~300 x D
            # matrix formed by the field vectors, where D is the dimensionality of the embeddings, and the length-D doc
            # vector. Naturally it's faster to create batches of N docs and take dot products 300 x D * D x N. This
            # yielded ~3x improvements.
            similarity = fields.score(embedding)
            avg_sim_values = zip_longest(fields.index, similarity.average().astype(float))
            f.write(json.dumps({
                'merged_id': record['merged_id'],
                'fields': [{'id': k, 'score': v} for k, v in avg_sim_values]
            }) + '\n')
            i += 1
            if i == limit:
                break
    stop_time = timeit.default_timer()
    elapsed = round(stop_time - start_time, 1)
    print(f'{limit} docs in {elapsed}s')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('lang', action='store_true')
    parser.add_argument('--timeit', action='store_true')
    args = parser.parse_args()
    if args.timeit:
        time(args.lang)
    else:
        field_model = FieldModel(args.lang)
        score(field_model, lang=args.lang)
