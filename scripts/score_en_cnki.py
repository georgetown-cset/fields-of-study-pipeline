"""
Calculate field scores for EN CNKI text.
"""
import argparse
import gzip
import json
import timeit
from pathlib import Path
from itertools import zip_longest

from fos.model import FieldModel
from fos.settings import CORPUS_DIR


def iter_extract(lang='en', corpus_dir=CORPUS_DIR):
    files = list(Path(corpus_dir).glob(f'{lang}_*.jsonl.gz'))
    assert files
    for file in files:
        with gzip.open(file, 'rb') as infile:
            for line in infile:
                if not line:
                    continue
                yield json.loads(line)


def main(limit=1000, bq_format=False):
    fields = FieldModel("en")
    start_time = timeit.default_timer()
    i = 0
    with open(CORPUS_DIR / f'cnki_en_scores.jsonl', 'wt') as f:
        for record in iter_extract(f'cnki_en_'):
            embedding = fields.embed(record['text'])
            sim = fields.score(embedding)
            avg_sim_values = zip_longest(fields.index, sim.average().astype(float))
            if bq_format:
                f.write(json.dumps({'document_name': record['document_name'],
                                    'fields': [{'id': k, 'score': v} for k, v in avg_sim_values]}) + '\n')
            else:
                avg_sim = {k: v for k, v in avg_sim_values}
                f.write(json.dumps({'document_name': record['document_name'], **avg_sim}) + '\n')
            i += 1
            if limit and (i == limit):
                break
    print(round(timeit.default_timer() - start_time))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Score EN CNKI text')
    parser.add_argument('--limit', type=int, default=10000, help='Record limit')
    parser.add_argument('--bq_format', action='store_true', help='If specified, will output nested field scores')
    args = parser.parse_args()
    main(limit=args.limit, bq_format=args.bq_format)
