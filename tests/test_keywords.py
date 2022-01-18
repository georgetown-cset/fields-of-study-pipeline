import numpy as np

from fos.keywords import load_entities, find_keywords, embed_entities


def test_embed_entities():
    trie = load_entities()
    entity_vector = embed_entities("natural language processing", trie)
    assert isinstance(entity_vector, np.ndarray)


def test_embed_nothing():
    trie = load_entities()
    result = embed_entities("", trie)
    assert result is None


def test_find_keywords():
    trie = load_entities()
    hits = list(find_keywords("aerides", trie))
    assert len(hits) == 1
    assert hits[0][0] == "aerides" and isinstance(hits[0][1], np.ndarray)
