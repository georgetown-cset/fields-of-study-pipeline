"""
Download EN CNKI text.
"""
import os
from pathlib import Path

from fos import gcp
from fos.corpus import CORPUS_DIR
from fos.gcp import write_query, extract_table, delete_blobs, set_default_clients
from fos.settings import QUERY_PATH, SQL_DIR

if Path.cwd().name == 'scripts':
    os.chdir('..')


def main(output_dir=CORPUS_DIR,
         query_path=SQL_DIR / 'en_cnki.sql',
         limit=1000,
         use_default_clients=True,
         bq_dest='field_model_replication',
         extract_bucket='fields-of-study',
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
    query_destination = f'{bq_dest}.cnki_en_corpus'
    extract_prefix = extract_prefix if extract_prefix else f'model-replication/cnki_en_corpus-'

    # we'll write to {output_dir}/{lang}.tsv; check up front the directory exists
    if not Path(output_dir).is_dir():
        raise NotADirectoryError(output_dir)

    # send the parameterized query to the API and wait for the result
    query = Path(query_path).read_text()
    if limit:
        query += f'\n limit {limit}'
    if use_default_clients:
        set_default_clients()
    write_query(query,
                query_destination,
                clobber=True)
    delete_blobs(extract_bucket, extract_prefix)
    extract_table(query_destination, f'gs://{extract_bucket}/{extract_prefix}*.jsonl.gz')
    gcp.download(extract_bucket, extract_prefix, output_dir)


if __name__ == '__main__':
    main()
