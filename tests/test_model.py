"""
Test that instantiating a FieldModel loads the expected assets and can embed text.
"""
import json

import numpy as np
import pandas as pd
from ahocorasick import Automaton
from fasttext.FastText import _FastText
from gensim.corpora import Dictionary
from gensim.similarities import MatrixSimilarity, SparseMatrixSimilarity
from gensim.sklearn_api import TfIdfTransformer

from fos.model import FieldModel, Embedding
from fos.settings import ASSETS_DIR


def test_create_field_model():
    # Test that we can instantiate a field model in both languages
    for lang in ['en']:
        fields = FieldModel(lang)
        assert isinstance(fields.fasttext, _FastText)
        assert isinstance(fields.tfidf, TfIdfTransformer)
        assert isinstance(fields.dictionary, Dictionary)
        assert isinstance(fields.entities, Automaton)
        assert isinstance(fields.field_fasttext, MatrixSimilarity)
        assert isinstance(fields.field_tfidf, SparseMatrixSimilarity)
        assert isinstance(fields.index, list)


def test_create_embedding():
    embedding = Embedding(
        fasttext=np.zeros((250,), dtype=np.float32),
        tfidf=[],
        entity=np.zeros((250,), dtype=np.float32))
    assert (embedding.fasttext == np.float32(0)).all()
    assert (embedding.tfidf == [])
    assert (embedding.entity == np.float32(0)).all()


def test_serialize_embedding():
    embedding = Embedding(
        fasttext=np.zeros((250,), dtype=np.float32),
        tfidf=[],
        entity=np.zeros((250,), dtype=np.float32))
    text = embedding.json(foo='bar')
    from_disk = json.loads(text)
    assert len(from_disk['fasttext']) == 250 and all([x == 0.0 for x in from_disk['fasttext']])
    assert len(from_disk['entity']) == 250 and all([x == 0.0 for x in from_disk['entity']])
    assert from_disk['tfidf'] == []
    assert from_disk['foo'] == 'bar'


def test_field_order():
    # The field order in meta.jsonl should match the order of the field keys
    fields = FieldModel()
    meta = pd.read_json(ASSETS_DIR / 'fields/field_meta.jsonl', lines=True)
    assert fields.index == list(meta['name'])
    # And the field order should be sorted by level and name
    meta.sort_values(['level', 'name'], inplace=True)
    assert fields.index == list(meta['name'])
