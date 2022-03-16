import contextlib
import gzip
import json
import os
import pickle
from pathlib import Path

import fasttext
import fasttext.util
import gensim
import pandas as pd
import pytest

from fos.settings import CORPUS_DIR, ASSETS_DIR
from fos.util import preprocess

TEST_ASSETS_DIR = Path(__file__).parent / 'assets'


@pytest.fixture
def texts():
    """Load example texts."""
    with open(TEST_ASSETS_DIR / 'texts.json', 'rt') as f:
        return json.load(f)


@pytest.fixture
def meta() -> pd.DataFrame:
    """Load field metadata."""
    with gzip.open(ASSETS_DIR / 'fields/fos.pkl.gz', 'rb') as f:
        df = pickle.load(f)
    return df


@pytest.fixture
def mag_outputs():
    """Load MAG outputs."""
    with open(CORPUS_DIR / 'mag-output.jsonl', 'rt') as f:
        return [json.loads(line) for line in f]


@pytest.fixture
def preprocessed_texts():
    """Load example texts and preprocess them."""
    with open(TEST_ASSETS_DIR / 'texts.json', 'rt') as f:
        texts_ = json.load(f)
        return {k: preprocess(v) for k, v in texts_.items()}


@pytest.fixture
def vanilla_en_fasttext():
    """Load the pretrained EN FastText model.

    On first run, this will download the model to the tests/assets directory.
    """
    with working_directory(TEST_ASSETS_DIR.absolute()):
        fasttext.util.download_model('en', if_exists='ignore')  # English
        return fasttext.load_model('cc.en.300.bin')


@pytest.fixture
def vanilla_en_word2vec():
    """Load the EN word2vec model pretrained on Google News.

    On first run, this will download the model to the tests/assets directory.
    """
    with working_directory(TEST_ASSETS_DIR.absolute()):
        return gensim.utils.downloader.api.load('word2vec-google-news-300')


@contextlib.contextmanager
def working_directory(path):
    """Change the working directory for the context, then change back to the original on exit."""
    prev_cwd = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(prev_cwd)
