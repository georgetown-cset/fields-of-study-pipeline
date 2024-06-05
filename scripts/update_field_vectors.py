"""
Apply l2 normalization to fasttext field vectors.

Instances of MatrixSimilarity classes have an attribute 'normalize' and it's true for our field vectors. This results
in normalization of the input document vector before similarity calculations. The field vectors are not normalized;
those have to be to normalized on creation. Originally I did this for the fasttext entity field vectors, but not the
fasttext entity vectors, unintentionally. This script updates them.
"""
import pickle

import numpy as np
from numpy.linalg import norm

from fos.model import FieldModel
from fos.settings import EN_FIELD_FASTTEXT_PATH, ZH_FIELD_FASTTEXT_PATH


def main():
    for lang in ["en", "zh"]:
        model = FieldModel(lang)

        norms = norm(model.field_fasttext.index, 2, axis=1)
        normalized_index = model.field_fasttext.index / norms[:, None]

        # ZH field vectors include some zeroed rows for missing fields
        normalized_index = np.nan_to_num(normalized_index)

        assert model.field_fasttext.index.shape == normalized_index.shape
        model.field_fasttext.index = normalized_index

        if lang == "en":
            output_path = EN_FIELD_FASTTEXT_PATH
        elif lang == "zh":
            output_path = ZH_FIELD_FASTTEXT_PATH
        with open(output_path, 'wb') as f:
            pickle.dump(model.field_fasttext, f)


if __name__ == "__main__":
    main()
