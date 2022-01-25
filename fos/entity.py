"""
Search for entity mentions in text and create entity embeddings.

The original MAG code defines embeddings for "entities" that were a subset of fields. They're 100d vectors that we
believe the MAG team created by averaging over the embeddings for each entity (i.e., field, more or less) mentioned in
field text. For instance, the "artificial intelligence" Wiki page mentions "machine learning," among other fields. Then
the entity embedding for the AI field is the average over word2vec embeddings for these mentioned fields.

We've done the same in our replication, but with FastText vectors. When embedding publication text, we search for entity
(field) mentions and then define its entity embedding as the average over the vectors of mentioned entities.
Specifically, we use Aho-Corasick over tokens, as implemented in pyahocorasick
(https://pyahocorasick.readthedocs.io/en/latest/).
"""
import json
import pickle
from typing import Tuple, Optional

import ahocorasick
import numpy as np

from fos.settings import ASSETS_DIR, FASTTEXT_DIM
from fos.util import norm_sum


def embed_entities(text, trie) -> Optional[np.ndarray]:
    """Embed entity mentions in text.

    Per the LanguageSimilarity code, the entity embedding is the l2-normed sum of the vectors for entities that appear
    in the text.

    :param text: Input text.
    :param trie: Entity vector trie.
    :return: A vector if any entity mentions are in the input text; otherwise None.
    """
    vectors = [v for _, (k, v) in find_keywords(text, trie)]
    if not len(vectors):
        return np.zeros(FASTTEXT_DIM, dtype=np.float32)
    return norm_sum(vectors)


def find_keywords(text: str, trie: ahocorasick.Automaton) -> Tuple[str, np.ndarray]:
    """Find in text the longest-matching entities in the trie.

    :return: Yields tuples of the matching key and its value.
    """
    for end_index, (k, v) in trie.iter_long(text):
        yield k, v


def load_entities(lang="en") -> ahocorasick.Automaton:
    with open(ASSETS_DIR / f'{lang}_entity_trie.pkl', 'rb') as f:
        trie = pickle.load(f)
    return trie


def read_trie(path):
    """Read LanguageSimilarity-formatted entity trie

    We've dumped this data from the MAG Language Similarity package as ``entityMatcher.json``. It's a trie of tokens
    in which the values are vectors.
    """
    with open(path, 'rt') as f:
        trie = json.load(f)
        yield from _flatten_trie(trie)


def _flatten_trie(node, ancestors=None):
    # Flatten a LanguageSimilarity trie
    if 'Children' not in node and 'IsWordEnd' not in node:
        for child, value in node.items():
            yield from _flatten_trie(value, [child])
    if node.get('IsWordEnd'):
        # We're at the end of a phrase
        yield tuple(ancestors), node['Value']
    if 'Children' in node:
        # Whether or not we're at the end of a phrase, it may be part of a longer phrase
        for child, value in node['Children'].items():
            yield from _flatten_trie(value, ancestors + [child])


def create_automaton(entities: dict):
    """Create an automaton for Aho-Corasick search from a dict."""
    trie = ahocorasick.Automaton()
    for k, v in entities.items():
        if isinstance(k, str):
            entity = k
        else:
            entity = ' '.join(k)
        trie.add_word(entity, (entity, v))
    trie.make_automaton()
    return trie
