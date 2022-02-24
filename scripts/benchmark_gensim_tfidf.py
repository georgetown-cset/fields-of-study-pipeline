import argparse
import timeit

from fos.model import FieldModel  # noqa
from fos.settings import CORPUS_DIR
from fos.util import iter_bq_extract
from fos.vectors import load_tfidf, embed_tfidf

tfidf, dictionary = load_tfidf('en')


def time():
    print(timeit.timeit('score()', globals=globals(), number=10))


def score(lang='en', limit=1000):
    i = 0
    start_time = timeit.default_timer()
    with open(CORPUS_DIR / f'{lang}_scores.jsonl', 'wt') as f:
        for record in iter_bq_extract(f'{lang}_'):
            vector = embed_tfidf(record['text'].split(), tfidf, dictionary)
            if i == limit:
                break
    stop_time = timeit.default_timer()
    elapsed = round(stop_time - start_time, 1)
    print(f'{limit} docs in {elapsed}s')


if __name__ == '__main__':
    time()
