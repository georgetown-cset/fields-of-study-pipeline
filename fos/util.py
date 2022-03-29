import csv
import gzip
import json
import os
import re
import string
import unicodedata
from pathlib import Path

import pandas as pd
from invoke import Context

from fos.settings import CORPUS_DIR

# We want to replace these whitespace characters with spaces
# fasttext expects newlines to represent document breaks, so they can't appear in the input to get_sentence_vector()
WS = '\t\n\r\v\x0b\x0c\u200B\u2060\uFEFF'

# Create a character map for this--adding ascii uppercase -> lowercase, and also dropping punctuation
TO_CLEAN_LOWER = str.maketrans(string.ascii_uppercase + WS,
                               string.ascii_lowercase + ' ' * len(WS),
                               string.punctuation)

# We'll remove lone numbers ('11' but not 'X11')
LONE_NUMBERS = re.compile(r'\b\d+\b')
NONBREAKING_SPACE = re.compile(r"[^\S\n\v]+")


def preprocess(text, lang='en'):
    if lang == 'en':
        # For English, remove everything but alpha, spaces, and digits; also remove lone numbers
        text = text.translate(TO_CLEAN_LOWER)
        # We now have lowercase text without punctuation but may have non-ascii chars; map where possible otherwise drop
        text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('ascii')
        text = LONE_NUMBERS.sub('', text.lower())
    elif lang == 'zh':
        # For Chinese, only remove punctuation
        text = text.translate(TO_CLEAN_LOWER)
    else:
        raise ValueError(lang)
    # normalizing whitespace ('   ' -> ' ') doesn't make a difference after tokenization but it's tidier when reviewing
    text = NONBREAKING_SPACE.sub(' ', text)
    return text.strip()


def iter_bq_extract(prefix, corpus_dir=CORPUS_DIR):
    files = list(Path(corpus_dir).glob(f'{prefix}*.jsonl.gz'))
    assert files
    for file in files:
        with gzip.open(file, 'rb') as infile:
            for line in infile:
                if not line:
                    continue
                yield json.loads(line)


def preprocess_text(record, lang="en"):
    text = ""
    if "title" in record and not pd.isnull(record["title"]):
        text += record["title"] + " "
    if "abstract" in record and not pd.isnull(record["abstract"]):
        text += record["abstract"]
    return preprocess(text, lang)


def read_output(path):
    """Read scoring output from JSONL."""
    output = {}
    with open(path, 'rt') as f:
        for line in f:
            record = json.loads(line)
            output[record['merged_id']] = record
    return output


def read_go_output(path):
    """Read output from the Go implementation.

    Output is a TSV with doc IDs in the first column and field scores in the rest. A header row gives the field IDs.
    If --all was passed, a second column named `score` indicates which scores the row gives: fastText, entity, tfidf, or
    field/average.
    """
    output = {}
    with open(path, 'rt') as f:
        reader = csv.DictReader(f, delimiter='\t')
        for row in reader:
            merged_id = row.pop('merged_id')
            if 'score' in row:
                method = row.pop('score')
                output[(merged_id, method)] = {int(k): float(v) for k, v in row.items()}
            else:
                output[merged_id] = {int(k): float(v) for k, v in row.items()}
    return output


def run_go(input, output, args="", queue=2, workers=2, bin_path=Path(os.getenv('GOFOS', "~/.go/src/corpus/fields"))):
    cmd = f"./fields score -i {input} -o {output} {args}"
    if queue is not None:
        cmd += f" --queue {queue} "
    if workers is not None:
        cmd += f" --workers {workers} "
    c = Context()
    with c.cd(bin_path.parent):
        print('Invoking:\n', cmd)
        result = c.run(cmd, in_stream=False)
        assert result.ok
    return result.stdout
