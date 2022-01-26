import numpy as np

from fos.entity import load_entities, find_keywords, embed_entities, create_automaton
from fos.settings import FASTTEXT_DIM


def test_embed_entities():
    # Entity embeddings are the average over the FastText embeddings for mentioned entities in text
    trie = load_entities()
    entity_vector = embed_entities("natural language processing", trie)
    assert isinstance(entity_vector, np.ndarray)
    assert entity_vector.shape == (FASTTEXT_DIM,)
    assert entity_vector.dtype == np.float32


def test_embed_nothing():
    # If there are no entities mentioned in the text, we return a zeroed array
    trie = load_entities()
    result = embed_entities("", trie)
    assert isinstance(result, np.ndarray)
    assert result.shape == (FASTTEXT_DIM,)
    assert result.dtype == np.float32
    assert (result == np.float32(0)).all()


def test_find_entities():
    # Trie keys are lowercased, and values are (field name, embedding) tuples
    trie = load_entities()
    assert 'engineering' in trie and 'engineering management' in trie
    # We find the longest match -- 'engineering management' and not 'engineering'
    entities = list(find_keywords('engineering management', trie))
    assert len(entities) == 1
    mention, (field_name, embedding) = entities[0]
    assert field_name == 'Engineering management'
    assert isinstance(embedding, np.ndarray)
    assert embedding.shape == (FASTTEXT_DIM,)
    assert embedding.dtype == np.float32


def test_find_uppercase():
    # Input text must be lowercased
    trie = load_entities()
    assert 'engineering' in trie and 'engineering management' in trie
    result = list(find_keywords('Engineering Management', trie))
    assert len(result) == 0


def test_create_automaton():
    entities = {'a': 0, 'b': 1}
    trie = create_automaton(entities)
    assert 'a' in trie and 'b' in trie
    result = {k: v for _, (k, v) in trie.iter_long('ab')}
    assert result == entities
