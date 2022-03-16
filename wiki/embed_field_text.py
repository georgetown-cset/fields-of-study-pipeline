"""
Create FastText and tfidf embeddings for fields from field text.
"""
import csv
import json
import pickle
from argparse import ArgumentParser

import dataset
import jieba
import numpy as np
from gensim.similarities import MatrixSimilarity, SparseMatrixSimilarity
from scipy.sparse import csr_matrix

from fos.settings import EN_FIELD_FASTTEXT_PATH, ZH_FIELD_FASTTEXT_PATH, EN_FIELD_TFIDF_PATH, ZH_FIELD_TFIDF_PATH, \
    EN_FIELD_KEY_PATH, ZH_FIELD_KEY_PATH, EN_FIELD_FASTTEXT_CSV, ZH_FIELD_FASTTEXT_CSV, EN_FIELD_TFIDF_JSON, \
    ZH_FIELD_TFIDF_JSON, EN_FIELD_TEXT, ZH_FIELD_TEXT
from fos.util import preprocess
from fos.vectors import load_fasttext, load_tfidf, embed_tfidf, sparse_norm, norm

db = dataset.connect('sqlite:///data/wiki.db')
table = db['pages']


def main(lang='en'):
    # Inputs: we need the fasttext model trained on the merged corpus and similarly our tfidftransformer + dict
    ft_model = load_fasttext(lang)
    tfidf, dictionary = load_tfidf(lang)

    # Outputs: FastText and tfidf field embeddings
    ft_embeddings = {}
    tfidf_embeddings = {}

    # Also: field content for debugging
    field_text = {}

    # Iterate over field IDs in stable order
    field_ids = sorted([field['id'] for field in table])
    # Make sure we're doing an integer sort
    assert isinstance(field_ids[0], int)

    for field_id in field_ids:
        field = table.find_one(id=field_id)
        field_id = field['id']
        text = field.get(f'{lang}_text', '')
        if text is None:
            text = ''
        if lang == 'zh' and field[f'en_text_mt'] is not None and len(field[f'en_text_mt']) > 0:
            text += ' ' + field['en_text_mt']
        name = field["display_name"]
        if not len(text):
            print(f'No {lang} text for {name}')
            continue
        clean_text = preprocess(text, lang)
        if not len(clean_text):
            print(f'No {lang} text for {name}')
            continue
        print(f'{name}: len {len(clean_text)}')
        if lang == 'zh':
            ft_embeddings[field_id] = norm(ft_model.get_sentence_vector('\t'.join(jieba.cut(clean_text))))
            tfidf_embeddings[field_id] = sparse_norm(embed_tfidf(jieba.cut(clean_text), tfidf, dictionary))
        else:
            ft_embeddings[field_id] = norm(ft_model.get_sentence_vector(clean_text))
            tfidf_embeddings[field_id] = sparse_norm(embed_tfidf(clean_text.split(), tfidf, dictionary))
        field_text[name] = clean_text

    # Write a matrix of fasttext vectors for fields (via `gensim.similarities.docsim.MatrixSimilarity`), for comparison
    # to fasttext publication vectors in scoring
    write_fasttext_similarity(ft_embeddings, lang)
    write_fasttext_csv(ft_embeddings, lang)

    # Similarly, write a matrix of tfidf vectors for fields for comparison to tfidf publication vectors in scoring
    write_tfidf_similarity(tfidf_embeddings, dictionary, lang)
    write_tfidf_csv(tfidf_embeddings, lang)

    # Write out field text for debugging ...
    write_field_text(field_text, lang)

    # Lastly write out the row order of these matrices ...
    assert list(ft_embeddings.keys()) == list(tfidf_embeddings.keys()) == field_ids
    write_field_keys(field_ids, lang)


def write_field_text(text, lang):
    """Write out the text used for field embeddings, for debugging."""
    if lang == 'en':
        output_path = EN_FIELD_TEXT
    elif lang == 'zh':
        output_path = ZH_FIELD_TEXT
    else:
        raise ValueError(lang)
    with open(output_path, 'wt') as f:
        json.dump(text, f, indent=2)
    print(f'Wrote {output_path}')


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
                                         num_features=len(dictionary), dtype=np.float64)
    with open(output_path, 'wb') as f:
        pickle.dump(tfidf_index, f)
    print(f'Wrote {output_path}')


def write_tfidf_csv(tfidf_embeddings, lang):
    """"Write to disk a CSV of tfidf vectors for fields in Go."""
    if lang == 'en':
        output_path = EN_FIELD_TFIDF_JSON
    elif lang == 'zh':
        output_path = ZH_FIELD_TFIDF_JSON
    else:
        raise ValueError(lang)
    with open(output_path, 'wt') as f:
        for field_id, vector in tfidf_embeddings.items():
            f.write(json.dumps({
                'id': field_id,
                'vector': [{"id": k, "value": v} for k, v in vector]
            }) + '\n')
    print(f'Wrote {output_path}')


def write_fasttext_similarity(ft_embeddings, lang):
    """"Write to disk a matrix of fasttext vectors for fields."""
    if lang == 'en':
        output_path = EN_FIELD_FASTTEXT_PATH
    elif lang == 'zh':
        output_path = ZH_FIELD_FASTTEXT_PATH
    else:
        raise ValueError(lang)
    vector_dim = ft_embeddings[next(iter(ft_embeddings))].size
    ft_similarity = MatrixSimilarity(ft_embeddings.values(), num_features=vector_dim, dtype=np.float64)
    for i, (k, v) in enumerate(ft_embeddings.items()):
        assert (v == ft_similarity.index[i,]).all()
    with open(output_path, 'wb') as f:
        pickle.dump(ft_similarity, f)
    print(f'Wrote {output_path}')


def write_fasttext_csv(ft_embeddings, lang):
    """"Write to disk a CSV of fasttext vectors for fields in Go."""
    if lang == 'en':
        output_path = EN_FIELD_FASTTEXT_CSV
    elif lang == 'zh':
        output_path = ZH_FIELD_FASTTEXT_CSV
    else:
        raise ValueError(lang)
    with open(output_path, 'wt') as f:
        writer = csv.writer(f, delimiter='\t')
        for field_id, vector in ft_embeddings.items():
            writer.writerow([field_id, *vector.tolist()])
    print(f'Wrote {output_path}')


def to_sparse(tfidf_vector, ncol):
    data = [x for i, x in tfidf_vector]
    rows = [0 for _ in tfidf_vector]
    cols = [i for i, x in tfidf_vector]
    shape = 1, ncol
    return csr_matrix((data, (rows, cols)), shape=shape)


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('--lang', default='en', choices=('en', 'zh', 'all'), help='Language')
    args = parser.parse_args()
    if args.lang == 'all':
        for lang in ['en', 'zh']:
            main(lang=lang)
    else:
        main(lang=args.lang)
