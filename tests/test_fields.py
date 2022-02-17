"""
Test the reimplementation of gensim's MatrixSimilarity functionality.
"""
from itertools import zip_longest

import numpy as np
from gensim import matutils
from gensim.similarities import SparseMatrixSimilarity
from scipy.sparse import csr_matrix

from fos.vectors import load_field_tfidf, embed_tfidf, load_tfidf, similarity, sparse_norm


def test_sparse_similarity(texts):
    tfidf, dictionary = load_tfidf("en")
    field_tfidf: SparseMatrixSimilarity = load_field_tfidf("en")
    assert isinstance(field_tfidf, SparseMatrixSimilarity)
    assert isinstance(field_tfidf.index, csr_matrix)
    for text in texts.values():
        vector = embed_tfidf(text.split(), tfidf, dictionary)
        docsim_similarity = field_tfidf[vector]
        rewrite_similarity = similarity(vector, field_tfidf.index)
        assert isinstance(docsim_similarity, np.ndarray)
        assert isinstance(rewrite_similarity, np.ndarray)
        assert (docsim_similarity == rewrite_similarity).all()


def test_sparse_norm(texts):
    tfidf, dictionary = load_tfidf("en")
    for text in texts.values():
        vector = embed_tfidf(text.split(), tfidf, dictionary)
        gensim_norm = matutils.unitvec(vector)
        rewrite_norm = sparse_norm(vector)
        for (k1, v1), (k2, v2) in zip_longest(gensim_norm, rewrite_norm):
            assert k1 == k2
            assert v1 == v2
