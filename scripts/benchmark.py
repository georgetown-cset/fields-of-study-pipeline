import timeit
import tempfile


from fos.util import iter_bq_extract
from fos.model import FieldModel

def main():
    print(timeit.timeit('embed(fields)', setup='fields = FieldModel("en")', globals=globals(), number=100))

def embed(fields, lang='en', limit=100):
    i = 0
    start_time = timeit.default_timer()
    with tempfile.TemporaryFile('wt') as f:
        for record in iter_bq_extract(f'{lang}_'):
            embedding = fields.embed(record['text'])
            embedding.dump_jsonl(f, merged_id=record['merged_id'])
            i += 1
            if i == limit:
                break
    stop_time = timeit.default_timer()
    elapsed = round(stop_time - start_time, 1)
    print(f'{limit} docs in {elapsed}s')


if __name__ == '__main__':
    main()
