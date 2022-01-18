from typing import Iterable

import numpy as np

ENTITY_WEIGHT = 0.5
TFIDF_WEIGHT = 0.5


def norm_sum(vectors: Iterable[np.ndarray]) -> np.ndarray:
    vector = np.sum(vectors, axis=0)
    l2_norm = np.linalg.norm(vector, 2, axis=0)
    if l2_norm == 0:
        return vector
    return vector / l2_norm


def convert_vector(v):
    if v is None:
        return None
    return np.array(v, dtype=np.float32)
