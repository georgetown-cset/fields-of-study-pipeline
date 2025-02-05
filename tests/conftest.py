import contextlib
import json
import os
from pathlib import Path

import fasttext
import fasttext.util
import gensim
import pandas as pd
import pytest

from fos.model import FieldModel
from fos.settings import ASSETS_DIR
from fos.util import read_go_output, run

TEST_ASSETS_DIR = Path(__file__).parent.absolute() / 'assets'
TEMP_GO_OUTPUT_PATH = Path("/tmp/fos.jsonl")


@pytest.fixture
def en_model() -> FieldModel:
    return FieldModel("en")


@pytest.fixture
def texts():
    """Load example texts."""
    with open(TEST_ASSETS_DIR / 'texts.json', 'rt') as f:
        return json.load(f)


@pytest.fixture
def meta() -> pd.DataFrame:
    """Load field metadata."""
    return pd.read_json(ASSETS_DIR / 'fields/field_meta.jsonl', lines=True)


@pytest.fixture
def preprocessed_texts():
    """Load example preprocessed EN texts."""
    with open(TEST_ASSETS_DIR / 'texts.json', 'rt') as f:
        return json.load(f)


@pytest.fixture
def preprocessed_jsonl_path():
    return str(TEST_ASSETS_DIR / 'texts.jsonl')


@pytest.fixture
def en_scores(preprocessed_texts, en_model):
    output = {}
    for doc_id, text in preprocessed_texts.items():
        output[doc_id] = en_model.run(text, dict_output=True)
    return output


def _score(texts, model):
    output = {}
    for doc_id, text in texts.items():
        output[doc_id] = model.run(text, dict_output=True)
    return output


@pytest.fixture
def en_go_scores():
    TEMP_GO_OUTPUT_PATH.unlink(missing_ok=True)
    run(f"go/fields score -i {TEST_ASSETS_DIR}/texts.jsonl -o {TEMP_GO_OUTPUT_PATH}", shell=True, check=True)
    scores = read_go_output(TEMP_GO_OUTPUT_PATH)
    return scores


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
