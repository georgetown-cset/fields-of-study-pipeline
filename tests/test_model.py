from ahocorasick import Automaton
from fasttext.FastText import _FastText
from gensim.corpora import Dictionary
from gensim.similarities import MatrixSimilarity, SparseMatrixSimilarity
from gensim.sklearn_api import TfIdfTransformer

from fos.model import FieldModel


def test_create_field_model():
    for lang in ['en', 'zh']:
        fields = FieldModel(lang)
        assert isinstance(fields.fasttext, _FastText)
        assert isinstance(fields.tfidf, TfIdfTransformer)
        assert isinstance(fields.dictionary, Dictionary)
        assert isinstance(fields.entities, Automaton)
        assert isinstance(fields.field_fasttext, MatrixSimilarity)
        assert isinstance(fields.field_tfidf, SparseMatrixSimilarity)
        assert isinstance(fields.index, list)
