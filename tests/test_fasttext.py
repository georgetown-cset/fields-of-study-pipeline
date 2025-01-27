import json

import numpy as np
from fasttext.FastText import _FastText, load_model

from fos.model import FieldModel
from fos.settings import EN_FASTTEXT_PATH, EN_FIELD_TEXT
from fos.vectors import load_fasttext, norm, embed_fasttext


def test_load_model():
    """Our wrapper for loading a model should be equivalent to using fasttext's load_model()."""
    vanilla_load = load_model(str(EN_FASTTEXT_PATH))
    assert isinstance(vanilla_load, _FastText)
    wrapped_load = load_fasttext("en")
    assert isinstance(wrapped_load, _FastText)
    assert len(wrapped_load.get_words()) and vanilla_load.get_words() == wrapped_load.get_words()


def test_field_embeddings(meta):
    """FastText field embeddings loaded by FieldModel should be the same as those from embedding the field text."""
    ft = load_model(str(EN_FASTTEXT_PATH))
    field_model = FieldModel("en")
    field_text = json.load(open(EN_FIELD_TEXT, "rt"))
    field_index = np.array(field_model.index)
    rows = set()
    for field, text in field_text.items():
        # Embed the field text with fasttext's get_sentence_vector
        ft_vector = ft.get_sentence_vector(text)
        # Look up the field ID associated with the field text
        field_id = meta.loc[meta.display_name == field].index[0]
        # Find the row of the field matrix that corresponds with the field ID
        field_model_row = np.where(field_id == field_index)[0][0]
        rows.add(field_model_row)
        # Slice into the field matrix to get the field vector
        field_model_vector = field_model.field_fasttext.index[field_model_row, :]
        # The normed vector from applying get_sentence_vector to the field text should be the same as the corresponding
        # row in the field matrix
        assert np.array_equiv(norm(ft_vector), field_model_vector)
    # And by iterating over all the field text, we iterated over all the rows of the field matrix
    assert len(rows) == len(field_text) == field_model.field_fasttext.index.shape[0]
    assert len(field_index) == field_model.field_fasttext.index.shape[0]


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
