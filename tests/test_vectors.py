from fasttext.FastText import _FastText
from gensim.corpora import Dictionary
from gensim.models import KeyedVectors
from gensim.sklearn_api import TfIdfTransformer

from fos.vectors import load_fasttext, load_tfidf, load_field_fasttext


def test_load_fasttext():
    for lang in ['en', 'zh']:
        model = load_fasttext(lang)
        assert isinstance(model, _FastText)


def test_load_tfidf():
    for lang in ['en', 'zh']:
        tfidf, dictionary = load_tfidf(lang)
        assert isinstance(tfidf, TfIdfTransformer)
        assert isinstance(dictionary, Dictionary)


def test_load_field_fasttext():
    for lang in ['en', 'zh']:
        fields = load_field_fasttext(lang)
        assert isinstance(fields, KeyedVectors)
