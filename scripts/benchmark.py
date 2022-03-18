import argparse
import json
import timeit
from itertools import zip_longest

from fos.model import FieldModel  # noqa
from fos.util import iter_bq_extract


def main():
    print(timeit.timeit('embed(fields)', setup='fields = FieldModel("en")', globals=globals(), number=10))


def embed(fields, lang='en', limit=1000):
    i = 0
    start_time = timeit.default_timer()
    with open('stream.json', 'wt') as f:
        for record in iter_bq_extract(f'{lang}_'):
            embedding = fields.embed(record['text'])
            similarity = fields.score(embedding)
            avg_sim_values = zip_longest(fields.index, similarity.average().astype(float))
            f.write(json.dumps({'merged_id': record['merged_id'],
                                'fields': [{'id': k, 'score': v} for k, v in avg_sim_values]}) + '\n')
            i += 1
            if i == limit:
                break
    stop_time = timeit.default_timer()
    elapsed = round(stop_time - start_time, 1)
    print(f'{limit} docs in {elapsed}s')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--profile', action='store_true')
    args = parser.parse_args()
    if args.profile:
        fields = FieldModel("en")
        embed(fields)
    else:
        main()
