"""
Test that expected assets exist.
"""
from fos import settings


def test_assets_exist():
    assert settings.EN_FASTTEXT_PATH.exists()
    assert settings.EN_TFIDF_PATH.exists()
    assert settings.EN_DICT_PATH.exists()

    assert settings.EN_FIELD_FASTTEXT_PATH.exists()
    assert settings.EN_FIELD_TFIDF_PATH.exists()
    assert settings.EN_FIELD_ENTITY_PATH.exists()
    assert settings.EN_FIELD_KEY_PATH.exists()
