from pathlib import Path

from google.cloud import bigquery
from google.cloud import translate
from more_itertools import chunked

from fos import gcp
from fos.gcp import write_query, extract_table, delete_blobs, set_default_clients
from fos.settings import CORPUS_DIR, QUERY_PATH

translation_client = None


def download(lang='en', output_dir=CORPUS_DIR, query_path=QUERY_PATH, limit=1000, skip_prev=False,
             use_default_clients=False, bq_dest='field_model_replication', extract_bucket='fields-of-study',
             extract_prefix=None):
    """Download a preprocessed corpus.

    :param lang: Language code, 'en' or 'zh'.
    :param output_dir: Directory for extract files.
    :param query_path: Path to SQL file.
    :param limit: Record limit.
    :param skip_prev: If true, skips unchanged records
    :param use_default_clients: If true, reads credentials from environment
    :param bq_dest: Dataset in BQ where data should be written
    :param extract_bucket: Bucket in GCS where exported jsonl should be written
    :param extract_prefix: GCS prefix where exported jsonl should be written within `extract_bucket`
    """
    query_destination = f'{bq_dest}.{lang}_corpus'
    extract_prefix = extract_prefix if extract_prefix else f'model-replication/{lang}_corpus-'

    # we'll write to {output_dir}/{lang}.tsv; check up front the directory exists
    if not Path(output_dir).is_dir():
        raise NotADirectoryError(output_dir)

    # send the parameterized query to the API and wait for the result
    query = Path(query_path).read_text()
    if skip_prev:
        query += (f'\n and merged_id not in '
                  f'(select clean_text.merged_id from clean_text '
                  f'inner join {bq_dest}.prev_{lang}_corpus '
                  f'on clean_text.merged_id = prev_{lang}_corpus.merged_id '
                  f'and clean_text.text = prev_{lang}_corpus.text)')
    if limit:
        query += f'\n limit {limit}'
    if use_default_clients:
        set_default_clients()
    write_query(query,
                query_destination,
                query_parameters=[bigquery.ScalarQueryParameter("lang", "STRING", lang)],
                clobber=True)
    delete_blobs(extract_bucket, extract_prefix)
    extract_table(query_destination, f'gs://{extract_bucket}/{extract_prefix}*.jsonl.gz')
    gcp.download(extract_bucket, extract_prefix, output_dir)


def batch_translate(texts, translator, batch_size=100, sleep=0):
    for batch in chunked(texts, batch_size):
        result = translator(batch)


class Translator:

    def __init__(self, source_lang='zh-CN', target_lang='en-US'):
        self.client = translate.TranslationServiceClient()
        self.parent = f"projects/gcp-cset-projects/locations/global"
        self.source_lang = source_lang
        self.target_lang = target_lang

    def translate(self, text):
        return self._make_request([text])[0]

    def translate_batch(self, texts, batch_size=100):
        for batch in chunked(texts, batch_size):
            yield self._make_request(batch)

    def translate_long(self, text):
        """Translate a long document by splitting it into chunks."""
        translation = ''
        for chars in chunked(text, 10_000):
            chunk = ''.join(chars)
            # https://cloud.google.com/translate/docs/supported-formats
            translation += ''.join(self._make_request([chunk]))
        return translation

    def _make_request(self, contents):
        response = self.client.translate_text(
            request={
                "parent": self.parent,
                "contents": contents,
                "mime_type": "text/plain",
                "source_language_code": self.source_lang,
                "target_language_code": self.target_lang,
            }
        )
        return [translation.translated_text for translation in response.translations]
