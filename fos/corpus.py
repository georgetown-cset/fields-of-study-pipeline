from pathlib import Path

from google.cloud import bigquery

from fos import gcp
from fos.gcp import write_query, extract_table, delete_blobs, set_default_clients
from fos.settings import CORPUS_DIR, QUERY_PATH


def download(lang='en', output_dir=CORPUS_DIR, query_path=QUERY_PATH, limit=1000, skip_prev=False,
             use_default_clients=False):
    """Download a preprocessed corpus.

    :param lang: Language code, 'en' or 'zh'.
    :param output_dir: Directory for extract files.
    :param query_path: Path to SQL file.
    :param limit: Record limit.
    :param skip_prev: If true, skips unchanged records
    :param use_default_clients: If true, reads credentials from environment
    """
    query_destination = f'field_model_replication.{lang}_corpus'
    extract_bucket = f'fields-of-study'
    extract_prefix = f'model-replication/{lang}_corpus-'

    # we'll write to {output_dir}/{lang}.tsv; check up front the directory exists
    if not Path(output_dir).is_dir():
        raise NotADirectoryError(output_dir)

    # send the parameterized query to the API and wait for the result
    query = Path(query_path).read_text()
    if skip_prev:
        query += (f'\n and merged_id not in '
                  f'(select clean_text.merged_id from clean_text '
                  f'inner join staging_new_fields_of_study.prev_en_corpus '
                  f'on clean_text.merged_id = prev_en_corpus.merged_id and clean_text.text = prev_en_corpus.text)')
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
