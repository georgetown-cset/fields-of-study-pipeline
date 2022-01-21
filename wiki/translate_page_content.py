"""

"""
import argparse
from itertools import zip_longest
from typing import List

from google.cloud import translate

client = translate.TranslationServiceClient()


def translate(texts: List[str]):
    """Translate texts from Chinese to English."""
    project_id = "gcp-cset-projects"
    location = "global"
    parent = f"projects/{project_id}/locations/{location}"

    # https://cloud.google.com/translate/docs/supported-formats
    response = client.translate_text(
        request={
            "parent": parent,
            "contents": texts,
            "mime_type": "text/plain",
            "source_language_code": "zh-CN",
            "target_language_code": "en-US",
        }
    )

    # Display the translation for each input text provided
    for text, translation in zip_longest(texts, response.translations):
        yield text, translation.translated_text


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('text')
    args = parser.parse_args()
    print(list(translate(['产业组织理论', '民族学'])))
