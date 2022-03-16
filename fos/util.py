import gzip
import json
import re
import string
import unicodedata
from pathlib import Path

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
