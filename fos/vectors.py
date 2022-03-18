"""

"""
import math
import pickle
from pathlib import Path
from typing import Tuple, List

from fasttext.FastText import _FastText
from gensim import matutils
from gensim.corpora import Dictionary
from gensim.similarities import MatrixSimilarity, SparseMatrixSimilarity
from gensim.sklearn_api import TfIdfTransformer

from fos.settings import EN_TFIDF_PATH, ZH_TFIDF_PATH, EN_FASTTEXT_PATH, ZH_FASTTEXT_PATH, EN_FIELD_FASTTEXT_PATH, \
    ZH_FIELD_FASTTEXT_PATH, EN_FIELD_TFIDF_PATH, ZH_FIELD_TFIDF_PATH, EN_DICT_PATH, ZH_DICT_PATH, EN_FIELD_KEY_PATH, \
    EN_FIELD_ENTITY_PATH, ZH_FIELD_ENTITY_PATH, ZH_FIELD_KEY_PATH
from fos.util import norm

ASSETS_DIR = Path(__file__).parent.parent / 'assets'


def embed_fasttext(text, model):
    vector = model.get_sentence_vector(text)
    if not len(vector):
        return []
    return norm(vector)


def embed_tfidf(text: List, tfidf: TfIdfTransformer, dictionary):
    bow = dictionary.doc2bow(text)
    if not len(bow):
        return []
    return tfidf.gensim_model[bow]


def load_tfidf(lang="en") -> Tuple[TfIdfTransformer, Dictionary]:
    if lang == "en":
        with open(EN_TFIDF_PATH, 'rb') as f:
            tfidf = pickle.load(f)
        dictionary = Dictionary.load_from_text(str(EN_DICT_PATH))
    elif lang == "zh":
        with open(ZH_TFIDF_PATH, 'rb') as f:
            tfidf = pickle.load(f)
        dictionary = Dictionary.load_from_text(str(ZH_DICT_PATH))
    else:
        raise ValueError(lang)
    return tfidf, dictionary


def load_fasttext(lang="en") -> _FastText:
    if lang == "en":
        path = EN_FASTTEXT_PATH
    elif lang == "zh":
        path = ZH_FASTTEXT_PATH
    else:
        raise ValueError(lang)
    # skip the warning that load_model prints
    return _FastText(model_path=str(path))


def load_field_fasttext(lang="en") -> MatrixSimilarity:
    if lang == "en":
        path = EN_FIELD_FASTTEXT_PATH
    elif lang == "zh":
        path = ZH_FIELD_FASTTEXT_PATH
    else:
        raise ValueError(lang)
    with open(path, 'rb') as f:
        return pickle.load(f)


def load_field_entities(lang="en") -> MatrixSimilarity:
    if lang == "en":
        path = EN_FIELD_ENTITY_PATH
    elif lang == "zh":
        path = ZH_FIELD_ENTITY_PATH
    else:
        raise ValueError(lang)
    with open(path, 'rb') as f:
        return pickle.load(f)


def load_field_keys(lang="en") -> List[str]:
    if lang == "en":
        path = EN_FIELD_KEY_PATH
    elif lang == "zh":
        path = ZH_FIELD_KEY_PATH
    else:
        raise ValueError(lang)
    with open(path, 'rt') as f:
        return [x.strip() for x in f if x.strip()]


def load_field_tfidf(lang="en") -> SparseMatrixSimilarity:
    if lang == "en":
        path = EN_FIELD_TFIDF_PATH
    elif lang == "zh":
        path = ZH_FIELD_TFIDF_PATH
    else:
        raise ValueError(lang)
    with open(path, 'rb') as f:
        return pickle.load(f)


def sparse_similarity(query, index):
    # gensim's sparse format looks like [(token_id, tfidf), (token_id, tfidf), ...]
    query = sparse_norm(query)
    # default case: query is a single vector, in sparse gensim format
    query = matutils.corpus2csc([query], index.shape[1], dtype=index.dtype)
    # compute cosine similarity against every other document in the collection
    result = index * query.tocsc()  # N x T * T x C = N x C
    # for queries of one document, return a 1d array
    result = result.toarray().flatten()
    return result


def batch_sparse_similarity(query, index):
    query = [sparse_norm(x) for x in query]
    query = matutils.corpus2csc(query, index.shape[1], dtype=index.dtype)
    # compute cosine similarity against every other document in the collection
    result = index * query.tocsc()  # N x T * T x C = N x C
    # avoid converting to dense array if maintaining sparsity
    return result.T


def sparse_norm(vector):
    # gensim sparse format looks like [(token_id, tfidf), (token_id, tfidf), ...]
    length = 1.0 * math.sqrt(sum(val ** 2 for _, val in vector))
    if length != 1.0:
        return [(term_id, x / length) for term_id, x in vector]
    else:
        return list(vector)
