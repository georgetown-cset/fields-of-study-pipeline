from typing import Dict

import numba
import numpy as np
from numba import types, typed
from numba.experimental import jitclass

from fos.settings import EN_DICT_PATH

str_to_int = (types.unicode_type, types.int64)
int_to_str = (types.int64, types.unicode_type)
int_to_int = (types.int64, types.int64)


@jitclass
class Vocabulary:
    token_to_id: Dict[str, int]
    id_to_token: Dict[int, str]
    id_to_frequency: Dict[int, int]
    id_to_idf: Dict[int, float]
    n_docs: int

    def __init__(self,
                 token_to_id: Dict[str, int],
                 id_to_token: Dict[int, str],
                 id_to_frequency: Dict[int, int],
                 id_to_idf: Dict[int, float],
                 n_docs: int):
        self.token_to_id = token_to_id
        self.id_to_token = id_to_token
        self.id_to_frequency = id_to_frequency
        self.id_to_idf = id_to_idf
        self.n_docs = n_docs

    def to_bow(self, text: str) -> Dict[int, int]:
        token_counts = typed.Dict.empty(key_type=types.int64, value_type=types.int64)
        tokens = text.split()
        for tok in tokens:
            # check for OOV token
            if tok not in self.token_to_id:
                continue
            # otherwise get the token ID
            token_id = self.token_to_id[tok]
            # implement a manual counter
            if token_id in token_counts:
                token_counts[token_id] += 1
            else:
                token_counts[token_id] = 1
        return token_counts


def load_vocab(path: str):
    token_to_id = typed.Dict.empty(key_type=types.unicode_type, value_type=types.int64)
    id_to_token = typed.Dict.empty(key_type=types.int64, value_type=types.unicode_type)
    id_to_frequency = typed.Dict.empty(key_type=types.int64, value_type=types.int64)
    id_to_idf = typed.Dict.empty(key_type=types.int64, value_type=types.float64)

    n_docs = 0

    i = 0
    for i, line in enumerate(open(path, 'rt')):
        if i == 0:
            # The first line should give the doc count
            # if not line.strip().isdigit():
            #     raise ValueError('expected corpus size: %s', line)
            n_docs = int(line.strip())
            continue
        # Each line after the first should give a tab-delimited triple: id, token, frequency:
        # It looks like: 1595530	hegemonizing	3
        token_id, token, freq = line.split('\t')
        token_id = int(token_id)
        freq = int(freq)
        # if token_id in id_to_token.keys():
        #     raise ValueError('token ID already defined: %s', token_id)
        # if token in token_to_id.keys():
        #     raise ValueError('token already defined: %s', token)
        token_to_id[token] = token_id
        id_to_token[token_id] = token
        id_to_frequency[token_id] = freq
        id_to_idf[token_id] = np.log2(np.float64(n_docs) / np.float64(freq))
    if not i > 0:
        raise ValueError('empty vocab')

    return Vocabulary(
        token_to_id=token_to_id,
        id_to_token=id_to_token,
        id_to_frequency=id_to_frequency,
        id_to_idf=id_to_idf,
        n_docs=n_docs)


if __name__ == '__main__':
    vocab = load_vocab(EN_DICT_PATH)
