"""
Process:

- Load the vector models (fasttext, tfidf, entity)

- Embed the publication text three ways -> up to three publication vectors.
    - We should always have a FastText vector
    - If all the publication tokens are OOV, our tfidf vector is zeroed but defined
    - But if there are no entity mentions, we don't have an entity embedding?
      -> Represent with a zeroed vector?

- Load the field embeddings (fasttext, tfidf, entity) as three matrices

- Calculate the similarities
"""
# cython: infer_types=True
# cython: language_level=3
cimport cython
import gensim
import numpy as np
from gensim import matutils

# @cython.boundscheck(False)  # Deactivate bounds checking
# @cython.wraparound(False)   # Deactivate negative indexing
def similarity(float[:, :] index, float[:] query):

    # is_corpus, query = gensim.utils.is_corpus(query)
    # if not matutils.ismatrix(query):
    #     if is_corpus:
    #         query = [matutils.unitvec(v) for v in query]
    #     else:
    #         query = matutils.unitvec(query)
    # gensim:
    # do a little transposition dance to stop numpy from making a copy of
    # self.index internally in numpy.dot (very slow).

    result = np.dot(index, query.T).T
    return result
