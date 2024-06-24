import csv
import gzip
import json
import re
import string
import subprocess
import unicodedata
from functools import partial
from pathlib import Path

import pandas as pd

from fos.settings import CORPUS_DIR, PIPELINES_DIR

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
    if not files:
        raise FileNotFoundError(f"No files found in {corpus_dir} match glob '{prefix}*.jsonl.gz'")
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

    Output is a JSONL with doc IDs in key 'merged_id' field scores as {"id": str, "score": float} dicts.
    Nested under key 'fields'.
    """
    output = {}
    with open(path, 'rt') as f:
        for line in f:
            record = json.loads(line)
            output[record['merged_id']] = {x['id']: x['score'] for x in record['fields']}
    return output


run = partial(subprocess.run, cwd=str(PIPELINES_DIR), capture_output=True, text=True)
