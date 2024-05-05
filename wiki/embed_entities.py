"""
Use FastText, the tfidf transformer, and field text to create entity embeddings.

We have two outputs: (1) an entity matcher that will efficiently find entity mentions in publication text and yield
the corresponding entity vectors, for creation of an entity-based publication embedding; and (2) a matrix of entity
vectors for fields (via `gensim.similarities.docsim.MatrixSimilarity`), for scoring purposes: comparison of
entity-based publication embeddings against entity-based field embeddings
"""
import pickle
from argparse import ArgumentParser
from collections import Counter

import dataset
import numpy as np
from gensim.similarities import MatrixSimilarity

from fos.entity import create_automaton, find_keywords
from fos.settings import ASSETS_DIR, EN_ENTITY_PATH, ZH_ENTITY_PATH, ZH_FIELD_ENTITY_PATH, EN_FIELD_ENTITY_PATH
from fos.vectors import load_field_fasttext, load_field_keys

VECTOR_DIM = 250

db = dataset.connect('sqlite:///wiki/data/wiki.db')
table = db['pages']


def main(lang='en', exclude_self_mentions=False):
    if not ASSETS_DIR.is_dir():
        # Fail early if output path won't be writable
        raise NotADirectoryError(ASSETS_DIR)

    # Create an automaton for searching field text for mentions of fields
    field_matcher = create_field_matcher(lang)
    # We'll be using the FastText field embeddings, so load that matrix and the index mapping row -> field ID
    field_vectors = load_field_fasttext(lang)
    keys = load_field_keys(lang)
    field_index = np.array(keys, dtype=object)
    # Container for fields => the frequency-weighted average of the mentioned fields in their text
    entity_vectors = {}
    id_to_title = {}

    # Iterate over each field ...
    for field in table:
        field_id = field['display_name']
        text = field[f'{lang}_text']
        titles = [field[f'en_title_{i}'] for i in range(1, 4)]
        entity_vector = np.zeros((VECTOR_DIM,), dtype=np.float32)

        # In English at L2+ and in Chinese starting with some fields in L1, we don't always have field text; these
        # entity embeddings will be zeroed
        if titles[0] is None and titles[1] is None and titles[2] is None:
            print(f'No {lang} page for {field["display_name"]}')
            entity_vectors[field_id] = entity_vector
            continue
        if text is None:
            print(f'No {lang} text for {field["display_name"]}')
            entity_vectors[field_id] = entity_vector
            continue
        # If we have a page title and text, we'll include the vector in the automaton
        # we can't include all our titles in the automaton so we're going to have to settle for just dropping
        # secondary and tertiary titles here and hoping it doesn't break our embeddings too much
        # TODO: determine if this is a problem
        id_to_title[field_id] = titles[0] if titles[0] is not None \
            else titles[1] if titles[1] is not None else titles[2]

        # Count how many times each entity is mentioned using the automaton created above
        entities = Counter([v for k, v in find_keywords(text.lower(), field_matcher)])
        for mention, count in entities.items():
            if (mention == titles[0].lower() or (titles[1] and mention == titles[1].lower())
                or (titles[2] and mention == titles[2].lower())) and exclude_self_mentions:
                # In text for a given field, that field tends to receive a lot of mentions. It isn't yet clear whether
                # the MAG team used the corresponding vectors in the entity vector, or excluded them, making the entity
                # embedding a representation of all *other* entity mentions. I believe they're included based on best
                # understanding of the motivation for the entity vectors.
                continue
            # Get the field ID for the mentioned entity, so we can slice the field vectors in the right place
            statement = f'''SELECT display_name from pages  where lower(en_title_1) = "{mention.lower()}" 
            or lower(en_title_2) = "{mention.lower()}" or lower(en_title_3) = "{mention.lower()}" LIMIT 1'''
            mention_id = None
            for row in db.query(statement):
                mention_id = row["display_name"]
            # Here `field_index == mention_id` gives us the single vector that corresponds to the mentioned field ID
            for _ in range(count):
                # We weight the vector by mention count. This is an IndexError if we don't find `mention_id` in
                # `field_index`, but we should have exactly 1 matching element unless this script is run out of order
                entity_vector += field_vectors.index[field_index == mention_id][0]

        l2_norm = np.linalg.norm(entity_vector, 2, axis=0)
        if l2_norm != 0:
            # Don't divide by zero
            entity_vector /= l2_norm
        entity_vectors[field_id] = entity_vector

    # We have two outputs: the first is an entity matcher that will efficiently find entity mentions in publication text
    # and yield the corresponding entity vectors, for creation of an entity-based publication embedding.
    write_entity_matcher(entity_vectors, id_to_title, lang)
    # The second output is a matrix of entity vectors for fields (via `gensim.similarities.docsim.MatrixSimilarity`),
    # for scoring purposes: comparison of entity-based publication embeddings against entity-based field embeddings
    write_entity_similarity(entity_vectors, field_index, lang)


def create_field_matcher(lang='en'):
    """Create an automaton for Aho-Corasick search of field names in field text.

    We don't really need this to be so fast, but we do want the search method to be consistent with what we'll use on
    publication text. Unlike the final entity matcher, this automaton doesn't have any useful values. We're just using
    it to find mentions.
    """
    fields = set([field[f'en_title_{i}'] for i in range(1, 4) for field in table if field[f'en_title_{i}']])
    return create_automaton({field.lower(): field for field in fields if field is not None})


def write_entity_matcher(entity_vectors, id_to_title, lang):
    """Create and write an entity matcher based on an entity => vector trie to disk.
    """
    # Transform the entity vector dict we just created into a trie usable in fast search over document text
    entity_matcher = create_automaton({
        id_to_title[field_id].lower(): (id_to_title[field_id], vector)
        for field_id, vector in entity_vectors.items()
        if field_id in id_to_title
    })
    if lang == 'en':
        output_path = EN_ENTITY_PATH
    # elif lang == 'zh':
    #     output_path = ZH_ENTITY_PATH
    else:
        raise ValueError(lang)
    with open(output_path, 'wb') as f:
        pickle.dump(entity_matcher, f)
    print(f'Wrote {lang} matcher to {output_path}')


def write_entity_similarity(entity_vectors, field_index, lang):
    """Create and write a field entity similarity matrix to disk.
    """
    # Rows need to be in the correct index order
    indexed_vectors = [entity_vectors[field_id] for field_id in field_index]
    entity_similarity = MatrixSimilarity(indexed_vectors, num_features=VECTOR_DIM, dtype=np.float32)
    if lang == 'en':
        output_path = EN_FIELD_ENTITY_PATH
    # elif lang == 'zh':
    #     output_path = ZH_FIELD_ENTITY_PATH
    else:
        raise ValueError(lang)
    with open(output_path, 'wb') as f:
        pickle.dump(entity_similarity, f)
    print(f'Wrote {lang} similarity matrix to {output_path}')


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('--lang', default='en', help='Language')
    parser.add_argument('--exclude_self_mentions', action='store_true', help='See comments')
    args = parser.parse_args()
    main(lang=args.lang, exclude_self_mentions=args.exclude_self_mentions)
