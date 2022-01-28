"""
Create FastText and tfidf embeddings for fields from field text.
"""
import pickle
from argparse import ArgumentParser

import dataset
import numpy as np
from gensim.similarities import MatrixSimilarity, SparseMatrixSimilarity
from scipy.sparse import csr_matrix

from fos.settings import EN_FIELD_FASTTEXT_PATH, ZH_FIELD_FASTTEXT_PATH, EN_FIELD_TFIDF_PATH, ZH_FIELD_TFIDF_PATH, \
    EN_FIELD_KEY_PATH, ZH_FIELD_KEY_PATH
from fos.util import preprocess
from fos.vectors import load_fasttext, load_tfidf, embed_tfidf

VECTOR_DIM = 250

db = dataset.connect('sqlite:///data/wiki.db')
table = db['pages']


def main(lang='en'):
    # Inputs: we need the fasttext model trained on the merged corpus and similarly our tfidftransformer + dict
    ft_model = load_fasttext(lang)
    tfidf, dictionary = load_tfidf(lang)

    # Outputs: FastText and tfidf field embeddings
    ft_embeddings = {}
    tfidf_embeddings = {}

    for field in table:
        field_id = field['id']
        text = field[f'{lang}_text']
        if text is None:
            # If we don't have any field text for this field, we use zeroed vectors
            ft_embeddings[field_id] = np.zeros((VECTOR_DIM,), dtype=np.float32)
            tfidf_embeddings[field_id] = []
            continue
        clean_text = preprocess(text, lang)
        ft_embeddings[field_id] = ft_model.get_sentence_vector(clean_text)
        tfidf_embeddings[field_id] = embed_tfidf(clean_text.split(), tfidf, dictionary)

    # Write a matrix of fasttext vectors for fields (via `gensim.similarities.docsim.MatrixSimilarity`), for comparison
    # to fasttext publication vectors in scoring
    write_fasttext_similarity(ft_embeddings, lang)

    # Similarly, write a matrix of tfidf vectors for fields for comparison to tfidf publication vectors in scoring
    write_tfidf_similarity(tfidf_embeddings, dictionary, lang)

    # Lastly write out the row order of these matrices; the order comes from iterating over database rows and will be
    # the same for both
    assert ft_embeddings.keys() == tfidf_embeddings.keys()
    write_field_keys(ft_embeddings.keys(), lang)


def write_field_keys(keys, lang):
    """Write out the row order of the field embedding matrices."""
    if lang == 'en':
        output_path = EN_FIELD_KEY_PATH
    elif lang == 'zh':
        output_path = ZH_FIELD_KEY_PATH
    else:
        raise ValueError(lang)
    with open(output_path, 'wt') as f:
        for k in keys:
            f.write(str(k) + '\n')
    print(f'Wrote {output_path}')


def write_tfidf_similarity(tfidf_embeddings, dictionary, lang):
    """"Write to disk a matrix of tfidf vectors for fields."""
    if lang == 'en':
        output_path = EN_FIELD_TFIDF_PATH
    elif lang == 'zh':
        output_path = ZH_FIELD_TFIDF_PATH
    else:
        raise ValueError(lang)
    tfidf_index = SparseMatrixSimilarity((to_sparse(v, len(dictionary)) for v in tfidf_embeddings.values()),
                                         num_features=len(dictionary), dtype=np.float32)
    with open(output_path, 'wb') as f:
        pickle.dump(tfidf_index, f)
    print(f'Wrote {output_path}')


def write_fasttext_similarity(ft_embeddings, lang):
    """"Write to disk a matrix of fasttext vectors for fields."""
    if lang == 'en':
        output_path = EN_FIELD_FASTTEXT_PATH
    elif lang == 'zh':
        output_path = ZH_FIELD_FASTTEXT_PATH
    else:
        raise ValueError(lang)
    ft_similarity = MatrixSimilarity(ft_embeddings.values(), num_features=VECTOR_DIM, dtype=np.float32)
    with open(output_path, 'wb') as f:
        pickle.dump(ft_similarity, f)
    print(f'Wrote {output_path}')


def to_sparse(tfidf_vector, ncol):
    data = [x for i, x in tfidf_vector]
    rows = [0 for _ in tfidf_vector]
    cols = [i for i, x in tfidf_vector]
    shape = 1, ncol
    return csr_matrix((data, (rows, cols)), shape=shape)


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('--lang', default='en', choices=('en', 'zh'), help='Language')
    args = parser.parse_args()
    main(lang=args.lang)
