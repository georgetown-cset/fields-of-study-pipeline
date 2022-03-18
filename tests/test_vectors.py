"""
Test that fasttext, tf-idf and entity vectors can be loaded.
"""
import ahocorasick
import gensim.similarities
from fasttext.FastText import _FastText
from gensim.corpora import Dictionary
from gensim.sklearn_api import TfIdfTransformer

from fos.entity import load_entities
from fos.vectors import load_fasttext, load_tfidf, load_field_fasttext, load_field_tfidf, load_field_entities


def test_load_fasttext():
    for lang in ['en', 'zh']:
        model = load_fasttext(lang)
        assert isinstance(model, _FastText)


def test_load_tfidf():
    for lang in ['en', 'zh']:
        tfidf, dictionary = load_tfidf(lang)
        assert isinstance(tfidf, TfIdfTransformer)
        assert isinstance(dictionary, Dictionary)


def test_load_entities():
    for lang in ['en', 'zh']:
        entities = load_entities(lang)
        assert isinstance(entities, ahocorasick.Automaton)


def test_load_field_fasttext():
    for lang in ['en', 'zh']:
        fields = load_field_fasttext(lang)
        assert isinstance(fields, gensim.similarities.MatrixSimilarity)


def test_load_field_tfidf():
    for lang in ['en', 'zh']:
        fields = load_field_tfidf(lang)
        assert isinstance(fields, gensim.similarities.SparseMatrixSimilarity)


def test_load_field_entities():
    for lang in ['en', 'zh']:
        fields = load_field_entities(lang)
        assert isinstance(fields, gensim.similarities.MatrixSimilarity)

def test_batch_embed_fasttext(texts):
    for lang in ['en', 'zh']:
        model = load_fasttext(lang)
        vectors = [model.get_sentence_vector(text) for text in texts.values()]

def test_batch_embed_tfidf(texts):
    for lang in ['en', 'zh']:
        tfidf, dictionary = load_tfidf(lang)
        bow = [dictionary.doc2bow(text.split()) for text in texts.values()]
        # __iter__ applies the transform
        dtm = [doc for doc in tfidf.gensim_model[bow]]
