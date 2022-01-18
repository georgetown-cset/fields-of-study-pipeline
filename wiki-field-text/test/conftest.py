from pathlib import Path

import pytest
from fasttext import load_model

from embed_field_text import read_tfidf

PROJECT_DIR = Path(__file__).parent.parent
EMBEDDINGS_DIR = PROJECT_DIR.parent / 'field-of-study-pipelines/assets/scientific-lit-embeddings'

EN_FT_PATH = EMBEDDINGS_DIR / 'english/fasttext/en_merged_model_120221.bin'
EN_TFIDF_PATH = EMBEDDINGS_DIR / 'english/tfidfs/tfidf_model_en_merged_sample.pkl'
EN_DICT_PATH = EMBEDDINGS_DIR / 'english/tfidfs/id2word_dict_en_merged_sample.txt'

ZH_FT_PATH = EMBEDDINGS_DIR / 'chinese/fasttext/zh_merged_model_011322.bin'
ZH_TFIDF_PATH = EMBEDDINGS_DIR / 'chinese/tfidfs/tfidf_model_zh_sample_011222.pkl'
ZH_DICT_PATH = EMBEDDINGS_DIR / 'chinese/tfidfs/id2word_dict_zh_sample_011222.txt'


@pytest.fixture(scope='session')
def en_fasttext():
    return load_model(str(EN_FT_PATH))


@pytest.fixture(scope='session')
def zh_fasttext():
    return load_model(str(ZH_FT_PATH))


@pytest.fixture(scope='session')
def en_tfidf():
    return read_tfidf(EN_TFIDF_PATH, EN_DICT_PATH)


@pytest.fixture(scope='session')
def zh_tfidf():
    return read_tfidf(ZH_TFIDF_PATH, ZH_DICT_PATH)
