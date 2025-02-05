"""
Test that fasttext, tf-idf and entity vectors can be loaded, and validate them.
"""
import ahocorasick
import gensim.similarities
from fasttext.FastText import _FastText
from gensim.corpora import Dictionary
from gensim.sklearn_api import TfIdfTransformer

from fos.entity import load_entities
from fos.vectors import load_fasttext, load_tfidf, load_field_fasttext, load_field_tfidf, load_field_entities


def test_load_fasttext():
    model = load_fasttext()
    assert isinstance(model, _FastText)


def test_load_tfidf():
    tfidf, dictionary = load_tfidf()
    assert isinstance(tfidf, TfIdfTransformer)
    assert isinstance(dictionary, Dictionary)


def test_load_entities():
    entities = load_entities()
    assert isinstance(entities, ahocorasick.Automaton)


def test_load_field_fasttext():
    fields = load_field_fasttext()
    assert isinstance(fields, gensim.similarities.MatrixSimilarity)


def test_load_field_tfidf():
    fields = load_field_tfidf()
    assert isinstance(fields, gensim.similarities.SparseMatrixSimilarity)


def test_load_field_entities():
    fields = load_field_entities()
    assert isinstance(fields, gensim.similarities.MatrixSimilarity)


def test_batch_embed_fasttext(texts):
    model = load_fasttext()
    vectors = [model.get_sentence_vector(text) for text in texts.values()]


def test_batch_embed_tfidf(texts):
    tfidf, dictionary = load_tfidf()
    bow = [dictionary.doc2bow(text.split()) for text in texts.values()]
    # __iter__ applies the transform
    dtm = [doc for doc in tfidf.gensim_model[bow]]
