"""
Create FastText and tfidf embeddings for fields from field text.
"""
import pickle
from argparse import ArgumentParser
from typing import Tuple

import dataset
import numpy as np
from gensim.similarities import MatrixSimilarity, SparseMatrixSimilarity
from scipy.sparse import csr_matrix
import pandas as pd

from fos.settings import EN_FIELD_FASTTEXT_PATH, EN_FIELD_TFIDF_PATH, EN_FIELD_KEY_PATH
from fos.util import preprocess, format_field_name
from fos.vectors import load_fasttext, load_tfidf, embed_tfidf, norm, ASSETS_DIR

db = dataset.connect('sqlite:///wiki/data/wiki.db')
table = db['pages']
refs = db['refs']


def main():
    # Inputs: we need the fasttext model trained on the merged corpus and similarly our tfidftransformer + dict
    ft_model = load_fasttext("en")
    tfidf, dictionary = load_tfidf("en")

    # We also need our table of field metadata: this is just the name and level of each final field.
    # Our wiki.db has a couple of fields that we don't want to include.
    meta = pd.read_json(ASSETS_DIR / "fields/field_meta.jsonl", lines=True)
    meta = meta.sort_values(['level', 'name'])
    meta = meta.set_index('name')
    meta['text'] = None
    meta['ft'] = None
    meta['tfidf'] = None

    names = []
    texts = []
    fts = []
    tfidfs = []
    for field in table:
        name, text = read_wiki_record(field, meta)
        if not text:
            # No text available
            continue
        if name in names:
            # Duplicate field name after normalization in DB
            continue
        names.append(name)
        texts.append(text)
        fts.append(norm(ft_model.get_sentence_vector(text)))
        tfidfs.append(embed_tfidf(text.split(), tfidf, dictionary))

    meta['text'] = texts
    meta['ft'] = fts
    meta['tfidf'] = tfidfs

    meta = meta.reset_index()
    meta.sort_values(['level', 'name'], inplace=True)
    meta['index'] = meta.index
    meta.to_json(ASSETS_DIR / "fields/field_meta_full.jsonl", lines=True, orient='records')

    # Write a matrix of fasttext vectors for fields (via `gensim.similarities.docsim.MatrixSimilarity`), for comparison
    # to fasttext publication vectors in scoring
    fasttext = {row['name']: row['ft'] for idx, row in meta.iterrows()}
    write_fasttext_similarity(fasttext, EN_FIELD_FASTTEXT_PATH)

    # Similarly, write a matrix of tfidf vectors for fields for comparison to tfidf publication vectors in scoring
    tfidf = {row['name']: row['tfidf'] for idx, row in meta.iterrows()}
    write_tfidf_similarity(tfidf, dictionary, EN_FIELD_TFIDF_PATH)

    # Lastly write out the row order of these matrices
    write_field_keys(meta['name'].to_list(), EN_FIELD_KEY_PATH)


def read_wiki_record(record: dict, meta: pd.DataFrame) -> Tuple[str, str]:
    field_id = format_field_name(record['display_name'])
    if field_id not in meta.index:
        print('Skipping', field_id, 'not in field metadata table (field_meta.jsonl)')
    text = record.get(f'en_text', '')
    if text is None:
        text = ''
    if not len(text):
        text = ''
    clean_text = preprocess(text, "en")
    if not len(clean_text):
        print(f'No en_text for {field_id}')
    return field_id, clean_text


def write_field_keys(keys, output_path):
    """Write out the row order of the field embedding matrices."""
    with open(output_path, 'wt') as f:
        for k in keys:
            f.write(str(k) + '\n')
    print(f'Wrote {output_path}')


def write_tfidf_similarity(tfidf_embeddings, dictionary, output_path):
    """"Write to disk a matrix of tfidf vectors for fields."""
    tfidf_index = SparseMatrixSimilarity((to_sparse(v, len(dictionary)) for v in tfidf_embeddings.values()),
                                         num_features=len(dictionary), dtype=np.float32)
    with open(output_path, 'wb') as f:
        pickle.dump(tfidf_index, f)
    print(f'Wrote {output_path}')


def write_fasttext_similarity(ft_embeddings, output_path):
    """Write to disk a matrix of fasttext vectors for fields."""
    vector_dim = next(iter(ft_embeddings.values())).shape[0]
    ft_similarity = MatrixSimilarity(ft_embeddings.values(), num_features=vector_dim, dtype=np.float32)
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
    main()
