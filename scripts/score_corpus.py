"""
"""
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


def main(lang="en", limit=1000, ndigits=6):
    fields = FieldModel(lang)
    start_time = timeit.default_timer()
    i = 0
    with open(CORPUS_DIR / f'{lang}_scores.jsonl', 'wt') as f:
        for record in iter_extract(lang):
            embedding = fields.embed(record['text'])
            sim = fields.score(embedding)
            avg_sim = {k: round(v, ndigits) for k, v in zip_longest(fields.index, sim.average().astype(float))}
            f.write(json.dumps({'merged_id': record['merged_id'], **avg_sim}) + '\n')
            i += 1
            if i == limit:
                break
    print(round(timeit.default_timer() - start_time))


if __name__ == '__main__':
    main()
