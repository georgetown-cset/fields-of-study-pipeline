import timeit

from fos.bow import load_vocab
from fos.settings import EN_DICT_PATH, CORPUS_DIR
from fos.util import iter_bq_extract
from fos.vectors import vectorize

vocab = load_vocab(EN_DICT_PATH)


def time():
    print(timeit.timeit(f'score()', setup='vectorize("testing 123", vocab, 1e-12)', globals=globals(), number=10))


def score(lang='en', limit=1000):
    i = 0
    start_time = timeit.default_timer()
    with open(CORPUS_DIR / f'{lang}_scores.jsonl', 'wt') as f:
        for record in iter_bq_extract(f'{lang}_'):
            vector = vectorize(record['text'], vocab, eps=1e-12)
            if i == limit:
                break
    stop_time = timeit.default_timer()
    elapsed = round(stop_time - start_time, 1)
    print(f'{limit} docs in {elapsed}s')


if __name__ == '__main__':
    time()
