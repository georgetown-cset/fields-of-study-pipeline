"""
Test the reimplementation of gensim docsim functionality.
"""
from itertools import zip_longest

import numpy as np
from gensim import matutils
from gensim.similarities import SparseMatrixSimilarity
from scipy.sparse import csr_matrix

from fos.vectors import load_field_tfidf, embed_tfidf, load_tfidf, sparse_similarity, sparse_norm


def test_sparse_similarity(texts):
    """Cosine similarity of tf-idf vectors should be the same when calculated by gensim or with our rewrite."""
    # Load tfidf vectorizer
    tfidf, dictionary = load_tfidf("en")
    # Load the field tfidf vectors
    field_tfidf: SparseMatrixSimilarity = load_field_tfidf("en")
    assert isinstance(field_tfidf, SparseMatrixSimilarity)
    assert isinstance(field_tfidf.index, csr_matrix)
    # For each test text ...
    for text in texts.values():
        # Embed via the tf-idf vectorizer
        vector = embed_tfidf(text.split(), tfidf, dictionary)
        # Calculate the similarity the original way: via the __getitem__ method of gensim's SparseMatrixSimilarity
        docsim_similarity = field_tfidf[vector]
        # Calculate it via our simpler reimplementation
        rewrite_similarity = sparse_similarity(vector, field_tfidf.index)
        # The result should be the same
        assert isinstance(docsim_similarity, np.ndarray)
        assert isinstance(rewrite_similarity, np.ndarray)
        assert np.array_equiv(docsim_similarity, rewrite_similarity)


def test_sparse_norm(texts):
    """Normalizing a tf-idf vector should have the same result with gensim's utility function or ours."""
    tfidf, dictionary = load_tfidf("en")
    for text in texts.values():
        # Create a sparse vector to test with
        vector = embed_tfidf(text.split(), tfidf, dictionary)
        # Apply gensim's utility function
        gensim_norm = matutils.unitvec(vector)
        # Apply our rewrite
        rewrite_norm = sparse_norm(vector)
        # We expect the same results
        for (gensim_term_id, gensim_tfidf), (rewrite_term_id, rewrite_tfidf) in zip_longest(gensim_norm, rewrite_norm):
            assert gensim_term_id == rewrite_term_id
            assert gensim_tfidf == rewrite_tfidf
