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
    else:
        raise ValueError(lang)
    # normalizing whitespace ('   ' -> ' ') doesn't make a difference after tokenization but it's tidier when reviewing
    text = NONBREAKING_SPACE.sub(' ', text)
    return text.strip()


def iter_bq_extract(prefix, corpus_dir=CORPUS_DIR):
    files = list(Path(corpus_dir).glob(f'{prefix}*.jsonl.gz'))
    if not files:
        raise FileNotFoundError(f"No files found in {corpus_dir} match glob '{prefix}*.jsonl.gz'")
    print(f"Found {len(files):,} files in {corpus_dir} matching glob '{prefix}*.jsonl.gz'")
    for file in sorted(files):
        with gzip.open(file, 'rb') as infile:
            print(f"Opened {file}")
            i = 0
            for line in infile:
                if not line:
                    continue
                yield json.loads(line)
                i += 1
            print(f"Read {i:,} records from {file}")


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


def format_field_name(text):
    """Format field name text by cleaning and casing it.
    """
    text = clean_field_name(text)
    text = case_field_name(text)
    return text


def clean_field_name(text):
    """Normalize field name text.
    """
    # Normalize hyphens
    text = re.sub(r'[–—]', '-', text)
    # Normalize whitespace
    text = re.sub(r'\s+', ' ', text)
    text = text.strip()
    return text


def case_field_name(text):
    """Apply title casing rules to field name text.
    """
    # We assume all dashes are hyphens and each whitespace is a space
    #   which requires clean_title_text first
    tokens = re.split(r'([ \-\(\)])', text)
    cased_tokens = []
    for i, token in enumerate(tokens):
        # Two split-pattern chars in a row yields an empty string between the matches
        if len(token) == 0:
            continue
        # Don't change abbreviation casing
        elif len(token) > 1 and token.isupper():
            cased_tokens.append(token)
        # Also don't change e.g. 'eWLB'
        elif not token[0].isupper() and len(token) > 1 \
                and any(c.isalpha() and c.isupper() for c in token[1:]):
            cased_tokens.append(token)
        # Lowercase prepositions, unless they're the first word
        elif i > 0 and token.lower() in ['a', 'an', 'the', 'of', 'and', 'or', 'but', 'for', 'nor', 'on', 'at', 'to',
                                         'from', 'by', 'with', 'in', 'through', 'via']:
            cased_tokens.append(token.lower())
        # Uppercase the first letter of other words
        elif len(token) == 1:
            cased_tokens.append(token[0].upper())
        else:
            cased_tokens.append(token[0].upper() + token[1:])
    text = ''.join(cased_tokens)
    return text
