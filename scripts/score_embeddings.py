"""
Calculate field scores for merged corpus embeddings.
"""
import json
import timeit
import argparse
from itertools import zip_longest

from fos.model import FieldModel, Embedding
from fos.settings import CORPUS_DIR


def main(lang="en", digits=6, limit=0):
    fields = FieldModel(lang)
    start_time = timeit.default_timer()
    i = 0
    with open(CORPUS_DIR / f'{lang}_embeddings.jsonl', 'rt') as fin, \
            open(CORPUS_DIR / f'{lang}_scores.jsonl', 'wt') as fout:
        for line in fin:
            record = json.loads(line)
            embedding = Embedding(fasttext=record['fasttext'], tfidf=record['tfidf'], entity=record['entity'])
            sim = fields.score(embedding)
            avg_sim = {k: round(v, digits) for k, v in zip_longest(fields.index, sim.average().astype(float))}
            fout.write(json.dumps({'merged_id': record['merged_id'], **avg_sim}) + '\n')
            i += 1
            if i == limit:
                break
    print(round(timeit.default_timer() - start_time))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Score merged corpus text')
    parser.add_argument('lang', choices=('en', 'zh'), help='Language')
    parser.add_argument('--digits', type=int, default=6, help='Float precision when serializing')
    args = parser.parse_args()
    main(lang=args.lang, digits=args.digits)
