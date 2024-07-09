"""
Dump text from the corpus to a text file with one doc per line, to benchmark the fastText CLI.
"""
import re

import typer

import gzip
import json
from pathlib import Path

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


def main(lang="en",
         limit=1000,
         corpus=CORPUS_DIR,
         output_path=None,
         ):
    if output_path is None:
        output_path = CORPUS_DIR / f'{lang}_texts.txt'
    i = 0
    with open(output_path, 'wt') as f:
        for record in iter_extract(lang, corpus):
            # Be very sure texts don't contain newlines since we'll rely on line numbers to match inputs and outputs
            text = re.sub(r"[\r\n\v]", " ", record['text'])
            f.write(text + '\n')
            i += 1
            if i == limit:
                break


if __name__ == '__main__':
    typer.run(main)
