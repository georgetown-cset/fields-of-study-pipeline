import numpy as np
from numba import types
from numba.experimental import jitclass

from fos.vectors import load_field_tfidf, load_field_fasttext, load_field_entities


@jitclass([
    ('fasttext', types.float32[:, :]),
    ('tfidf', types.float32[:, :]),
    ('entity', types.float32[:, :]),
])
class Fields:
    tfidf: np.ndarray
    fasttext: np.ndarray
    entity: np.ndarray

    def __init__(self,
                 tfidf: np.ndarray,
                 fasttext: np.ndarray,
                 entity: np.ndarray):
        self.tfidf = tfidf
        self.fasttext = fasttext
        self.entity = entity


def load_fields(lang='en'):
    fasttext = load_field_fasttext(lang)
    tfidf = load_field_tfidf(lang)
    entity = load_field_entities(lang)
    fields = Fields(tfidf=tfidf.index.todense(),
                    fasttext=fasttext.index,
                    entity=entity.index)
    return fields
