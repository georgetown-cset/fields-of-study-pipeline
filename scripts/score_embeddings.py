"""
Run the pipeline.
"""
import csv
import json
import timeit

from fos.model import FieldModel, Embedding
from fos.settings import CORPUS_DIR


def main(lang="en", digits=6):
    fields = FieldModel(lang)
    start_time = timeit.default_timer()
    with open(CORPUS_DIR / f'{lang}_embeddings.jsonl', 'rt') as fin, \
            open(CORPUS_DIR / f'{lang}_scores.jsonl', 'wt') as fout:
        for line in fin:
            record = json.loads(line)
            embedding = Embedding(fasttext=record['fasttext'], tfidf=record['tfidf'], entity=record['entity'])
            sim = fields.score(embedding)
            fout.write(json.dumps({'merged_id': record['merged_id'], **sim.average()}) + '\n')
    print(round(timeit.default_timer() - start_time))


if __name__ == '__main__':
    main()
