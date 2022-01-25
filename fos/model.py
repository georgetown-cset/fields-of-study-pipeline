import json
import logging
from typing import List, Tuple, Optional

import numpy as np

from fos.entity import load_entities, embed_entities
from fos.util import convert_vector
from fos.vectors import load_tfidf, load_fasttext, load_field_fasttext, load_field_tfidf, load_field_keys, \
    embed_fasttext, embed_tfidf, load_field_entities

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class Embedding:

    def __init__(self, fasttext: np.ndarray, tfidf: List[Tuple[int, float]], entity: np.ndarray):
        """Container for publication embeddings.

        :param fasttext: FastText embedding, as from ``embed_fasttext()``.
        :param tfidf: tf-idf embedding, as from ``embed_tfidf()``.
        :param entity: Entity embedding, as from ``embed_entities()``.
        """
        self.fasttext = convert_vector(fasttext)
        self.tfidf = tfidf
        self.entity = convert_vector(entity)

    def json(self, digits=6, **kw) -> str:
        """Serialize as JSON.

        :param digits: Round to this many digits.
        :param kw: Additional keywords added to the JSON object.
        """
        obj = dict(**kw)
        for attr in ['fasttext', 'entity']:
            if getattr(self, attr) is not None:
                obj[attr] = [round(x, digits) for x in getattr(self, attr).astype(float)]
            else:
                obj[attr] = None
        if self.tfidf is not None:
            obj['tfidf'] = [(token_id, round(tfidf, digits)) for token_id, tfidf in self.tfidf]
        else:
            obj['tfidf'] = None
        return json.dumps(obj, separators=(',', ': '))

    def dump_jsonl(self, f, digits=6, **kw) -> None:
        """Write JSONL serialization to file.

        :param f: File handle.
        :param digits:  Round to this many digits.
        :param kw: Additional keywords added to the JSON object.
        """
        f.write(self.json(digits=digits, **kw) + '\n')


class Similarity:

    def __init__(self, fasttext: np.ndarray, tfidf: np.ndarray, entity: np.ndarray):
        """Container for publication embeddings.

        :param fasttext: FastText publication-field similarities.
        :param tfidf: tf-idf publication-field similarities.
        :param entity: Entity publication-field similarities.
        """
        self.fasttext = fasttext
        self.tfidf = tfidf
        self.entity = entity

    def average(self) -> Optional[np.ndarray]:
        """Average the FastText, tf-idf and entity similarities, yielding field scores."""
        defined = [x for x in [self.fasttext, self.tfidf, self.entity] if x is not None]
        if not len(defined):
            return None
        else:
            return np.average(defined, axis=0)


class FieldModel(object):

    def __init__(self, lang="en"):
        """A 'model' for field scoring.

        :param lang: Language, 'en' or 'zh'.
        """
        logger.debug('Loading FieldModel assets')

        # Vectors for embedding publications
        self.fasttext = load_fasttext(lang)
        self.tfidf, self.dictionary = load_tfidf(lang)
        self.entities = load_entities(lang)

        # Field embeddings
        self.field_fasttext = load_field_fasttext(lang)
        self.field_tfidf = load_field_tfidf(lang)
        self.field_entities = load_field_entities(lang)

        # Field embedding index (gives the field IDs corresponding with field score vector elements)
        self.index = load_field_keys(lang)

    def embed(self, text: str) -> Embedding:
        """Embed publication text three ways."""
        return Embedding(
            fasttext=embed_fasttext(text, self.fasttext),
            tfidf=embed_tfidf(text.split(), self.tfidf, self.dictionary),
            entity=embed_entities(text, self.entities))

    def score(self, embedding: Embedding) -> Similarity:
        """Calculate field scores from a publication's embeddings."""
        if embedding.fasttext is not None:
            fasttext = self.field_fasttext[embedding.fasttext]
        else:
            fasttext = None
        if embedding.tfidf is not None:
            tfidf = self.field_tfidf[embedding.tfidf]
        else:
            tfidf = None
        if embedding.entity is not None and len(embedding.entity):
            entity = self.field_entities[embedding.entity]
        else:
            entity = None
        return Similarity(fasttext=fasttext, tfidf=tfidf, entity=entity)
