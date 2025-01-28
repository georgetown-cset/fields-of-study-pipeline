"""
Run the pipeline.

Input: preprocessed text as JSON or CSV

For each input doc:
- FT-embed
- Entity-embed
- Tfidf-embed -> probably need cython for speed

Output: all three embeddings. (Desirable because embedding the corpus is presumably expensive?)

For each field L0-L1 + candidate fields:
- Calculate similarity
- Average the similarities

Output: ~300 field scores for each doc.
"""
import argparse
import timeit

from fos.model import FieldModel
from fos.settings import CORPUS_DIR
from fos.util import iter_bq_extract


def main(lang="en", limit=0):
    fields = FieldModel(lang)
    start_time = timeit.default_timer()
    i = 0
    with open(CORPUS_DIR / 'en_embeddings.jsonl', 'wt') as f:
        for record in iter_bq_extract(f'{lang}_'):
            embedding = fields.embed(record['text'])
            embedding.dump_jsonl(f, merged_id=record['merged_id'])
            i += 1
            if i == limit:
                break
    print(round(timeit.default_timer() - start_time))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Embed merged corpus text')
    parser.add_argument('lang', choices=('en',), help='Language')
    parser.add_argument('--limit', type=int, default=0, help='Record limit')
    args = parser.parse_args()
    main(lang=args.lang, limit=args.limit)
