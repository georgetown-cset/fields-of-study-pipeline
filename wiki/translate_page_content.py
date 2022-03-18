"""

"""
import argparse
from itertools import zip_longest
from typing import List

import time
from tqdm import tqdm
import dataset
from google.cloud import translate
from more_itertools import chunked

client = translate.TranslationServiceClient()

db = dataset.connect('sqlite:///data/wiki.db')
table = db['pages']


def main():
    for field in tqdm(table):
        # We don't have ZH text for all fields, so check before hitting the API
        # We also skip the field if we've already translated its ZH text (change this if updating ZH text)
        translate_field(field, 'zh_text', zh_to_en, sleep=5)
        translate_field(field, 'en_text', en_to_zh, sleep=5)


def translate_field(field, text_col, translator, sleep=0):
    if field[text_col] is not None and field.get(text_col + '_mt') is None:
        if not field[text_col].strip():
            return
        translation = translator(field[text_col])
        table.update({'id': field['id'], text_col + '_mt': translation}, ['id'])
        time.sleep(sleep)


def zh_to_en(text: str):
    """Translate texts from Chinese to English."""
    return _translate(text, source='zh-CN', target='en-US')


def en_to_zh(text: str):
    """Translate texts from English to Chinese."""
    return _translate(text, source='en-US', target='zh-CN')


def _translate(text: str, source: str, target: str):
    """Request a translation text."""
    project_id = "gcp-cset-projects"
    location = "global"
    parent = f"projects/{project_id}/locations/{location}"

    translation = ''
    for chars in chunked(text, 10_000):
        chunk = ''.join(chars)
        # https://cloud.google.com/translate/docs/supported-formats
        response = client.translate_text(
            request={
                "parent": parent,
                "contents": [chunk],
                "mime_type": "text/plain",
                "source_language_code": source,
                "target_language_code": target,
            }
        )
        translation += ''.join((text.translated_text for text in response.translations))
    return translation


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('text')
    args = parser.parse_args()
    main()
