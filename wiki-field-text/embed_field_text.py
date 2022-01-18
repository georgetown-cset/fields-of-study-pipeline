import csv
import pickle
from argparse import ArgumentParser
from pathlib import Path

import numpy as np
from gensim.similarities import MatrixSimilarity, SparseMatrixSimilarity
from scipy.sparse import csr_matrix
from tqdm import tqdm

from fos.keywords import load_entities, embed_entities
from fos.vectors import load_fasttext, load_tfidf, embed_tfidf


def main(lang='en', max_level=1):
    ft_model = load_fasttext(lang)
    tfidf, dictionary = load_tfidf(lang)
    entities = load_entities(lang)

    fields = {row['id']: row for row in csv.DictReader(open('fields.tsv', 'rt'), delimiter='\t')}

    print(f'Reading field text content')
    field_paths = list(Path(lang).glob('*.txt'))
    if not field_paths:
        raise FileNotFoundError(f'{lang}/*.txt')

    print(f'Found {len(field_paths):,} fields; applying max_level={max_level}')
    ft_embeddings = {}
    tfidf_embeddings = {}
    entity_embeddings = {}
    for path in tqdm(field_paths):
        # fasttext expects 1 sentence per line; we get an error if there are newlines in the text
        text = path.read_text().replace('\n', ' ').replace('\r', ' ')
        field_id = path.stem
        if int(fields[field_id]['level']) > max_level:
            continue
        ft_embeddings[field_id] = ft_model.get_sentence_vector(text)
        tfidf_embeddings[field_id] = embed_tfidf(text.split(), tfidf, dictionary)
        entity_embeddings[field_id] = embed_entities(text, entities)

    ft_index = MatrixSimilarity(ft_embeddings.values(), num_features=250, dtype=np.float32)
    with open(f'{lang}_field_fasttext_similarity.pkl', 'wb') as f:
        pickle.dump(ft_index, f)

    entity_index = MatrixSimilarity(entity_embeddings.values(), num_features=100, dtype=np.float32)
    with open(f'{lang}_field_entity_similarity.pkl', 'wb') as f:
        pickle.dump(entity_index, f)

    tfidf_index = SparseMatrixSimilarity((to_sparse(v, len(dictionary)) for v in tfidf_embeddings.values()),
                                         num_features=len(dictionary), dtype=np.float32)
    with open(f'{lang}_field_tfidf_similarity.pkl', 'wb') as f:
        pickle.dump(tfidf_index, f)

    with open(f'{lang}_field_keys.txt', 'wt') as f:
        for k in ft_embeddings.keys():
            f.write(k + '\n')


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
