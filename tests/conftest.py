import contextlib
import json
import os
from pathlib import Path

import fasttext
import fasttext.util
import gensim
import pytest

from fos.bow import load_vocab
from fos.settings import EN_DICT_PATH, ZH_DICT_PATH
from fos.util import preprocess
from fos.vectors import load_tfidf

ASSETS_DIR = Path(__file__).parent / 'assets'


@pytest.fixture
def en_vocab():
    return load_vocab(EN_DICT_PATH)

@pytest.fixture
def en_tfidf():
    return load_tfidf('en')

@pytest.fixture
def en_dict():
    tfidf, dictionary = load_tfidf('en')
    return dictionary




@pytest.fixture
def zh_vocab():
    return load_vocab(ZH_DICT_PATH)


@pytest.fixture
def texts():
    """Load example texts."""
    with open(ASSETS_DIR / 'texts.json', 'rt') as f:
        return json.load(f)


@pytest.fixture
def preprocessed_texts():
    """Load example texts and preprocess them."""
    with open(ASSETS_DIR / 'texts.json', 'rt') as f:
        texts_ = json.load(f)
        return {k: preprocess(v) for k, v in texts_.items()}


@pytest.fixture
def vanilla_en_fasttext():
    """Load the pretrained EN FastText model.

    On first run, this will download the model to the tests/assets directory.
    """
    with working_directory(ASSETS_DIR.absolute()):
        fasttext.util.download_model('en', if_exists='ignore')  # English
        return fasttext.load_model('cc.en.300.bin')


@pytest.fixture
def vanilla_en_word2vec():
    """Load the EN word2vec model pretrained on Google News.

    On first run, this will download the model to the tests/assets directory.
    """
    with working_directory(ASSETS_DIR.absolute()):
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
