from pathlib import Path

from google.cloud import bigquery

from fos import gcp
from fos.gcp import write_query, extract_table, delete_blobs
from fos.settings import CORPUS_DIR, QUERY_PATH


def download(lang='en', output_dir=CORPUS_DIR, query_path=QUERY_PATH, limit=1000):
    """Download a preprocessed corpus.

    :param lang: Language code, 'en' or 'zh'.
    :param output_dir: Directory for extract files.
    :param query_path: Path to SQL file.
    :param limit: Record limit.
    """
    query_destination = f'field_model_replication.{lang}_corpus'
    extract_bucket = f'fields-of-study'
    extract_prefix = f'model-replication/{lang}_corpus-'

    # we'll write to {output_dir}/{lang}.tsv; check up front the directory exists
    if not Path(output_dir).is_dir():
        raise NotADirectoryError(output_dir)

    # send the parameterized query to the API and wait for the result
    query = Path(query_path).read_text()
    if limit:
        query += f'\n limit {limit}'
    write_query(query,
                query_destination,
                query_parameters=[bigquery.ScalarQueryParameter("lang", "STRING", lang)],
                clobber=True)
    delete_blobs(extract_bucket, extract_prefix)
    extract_table(query_destination, f'gs://{extract_bucket}/{extract_prefix}*.jsonl.gz')
    gcp.download(extract_bucket, extract_prefix, output_dir)
