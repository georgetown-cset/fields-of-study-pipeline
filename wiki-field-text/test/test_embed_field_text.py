from fasttext.FastText import _FastText
from gensim.corpora import Dictionary
from gensim.sklearn_api.tfidf import TfIdfTransformer


def test_load_en_ft(en_fasttext):
    assert isinstance(en_fasttext, _FastText)


def test_load_zh_ft(zh_fasttext):
    assert isinstance(zh_fasttext, _FastText)


def test_load_en_tfidf(en_tfidf):
    tfidf, dictionary = en_tfidf
    assert isinstance(tfidf, TfIdfTransformer)
    assert isinstance(dictionary, Dictionary)


def test_load_zh_tfidf(zh_tfidf):
    tfidf, dictionary = zh_tfidf
    assert isinstance(tfidf, TfIdfTransformer)
    assert isinstance(dictionary, Dictionary)
