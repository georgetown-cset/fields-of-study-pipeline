"""
Run the pipeline on EN CNKI. Modified from embed_corpus.py.

Output: ~300 field scores for each doc.
"""
import argparse
import timeit

from fos.model import FieldModel
from fos.settings import CORPUS_DIR
from fos.util import iter_bq_extract


def main(limit=0):
    fields = FieldModel("en")
    start_time = timeit.default_timer()
    i = 0
    with open(CORPUS_DIR / 'cnki_en_embeddings.jsonl', 'wt') as f:
        for record in iter_bq_extract(f'cnki_en_'):
            embedding = fields.embed(record['text'])
            embedding.dump_jsonl(f, document_name=record['document_name'])
            i += 1
            if i == limit:
                break
    print(round(timeit.default_timer() - start_time))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Embed merged corpus text')
    parser.add_argument('--limit', type=int, default=0, help='Record limit')
    args = parser.parse_args()
    main(limit=args.limit)
