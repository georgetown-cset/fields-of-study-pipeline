import json

import numpy as np
from fasttext.FastText import _FastText, load_model

from fos.model import FieldModel
from fos.settings import EN_FASTTEXT_PATH
from fos.vectors import load_fasttext, norm, embed_fasttext


def test_load_model():
    """Our wrapper for loading a model should be equivalent to using fasttext's load_model()."""
    vanilla_load = load_model(str(EN_FASTTEXT_PATH))
    assert isinstance(vanilla_load, _FastText)
    wrapped_load = load_fasttext("en")
    assert isinstance(wrapped_load, _FastText)
    assert len(wrapped_load.get_words()) and vanilla_load.get_words() == wrapped_load.get_words()


def test_fasttext_similarity(texts):
    """Similarities for FT vectors from the FieldModel via gensim's MatrixSimilarity should be the same as via numpy."""
    # We don't get *exactly* the same similarities, I think because
    eps = 1e-6
    for lang in ['en']:
        if lang == 'en':
            ft = load_model(str(EN_FASTTEXT_PATH))
        else:
            raise ValueError(lang)
        fields = FieldModel(lang)
        for text in texts.values():
            # Embed via the fasttext model that the FieldModel loaded
            model_vector = embed_fasttext(text, fields.fasttext)
            # Also via the fasttext model we loaded directly
            ft_vector = ft.get_sentence_vector(text)
            # This uses gensim's MatrixSimilarity method
            gensim_scores = fields.field_fasttext[model_vector]
            # It should be equivalent to this
            numpy_scores = np.dot(fields.field_fasttext.index, model_vector)
            # Or this, using the fasttext model loaded directly
            ft_numpy_scores = np.dot(fields.field_fasttext.index, norm(ft_vector))
            assert (np.abs(gensim_scores - numpy_scores) < eps).all()
            assert (np.abs(gensim_scores - ft_numpy_scores) < eps).all()
