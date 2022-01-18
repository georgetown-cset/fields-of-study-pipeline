"""
Some wrappers around the BQ and GCS clients for Python.

We authenticate using a service account keyfile, probably. Download like::

    $ gcloud iam service-accounts keys create key.json \
        --iam-account  jd1881-sdk-cli@gcp-cset-projects.iam.gserviceaccount.com

``KEY_PATH`` gives the path to the keyfile. Assign ``None`` to try authenticating with user credentials.

Reference:
    - https://googleapis.dev/python/bigquery/latest/index.html
    - https://googleapis.dev/python/storage/latest/client.html
"""
import warnings
from pathlib import Path
from typing import Union, Optional, List

import google.auth
from google.cloud import bigquery, storage
from google.cloud.bigquery import ExtractJobConfig, SchemaField, Table
from google.cloud.bigquery.job import QueryJob
from google.oauth2 import service_account
from tqdm import tqdm

PROJECT_ID = 'gcp-cset-projects'
KEY_PATH = Path(__file__).parent / '../key.json'

_bq_client = None
_storage_client = None
_credentials = None


def create_bq_client(key_path: Optional[str] = KEY_PATH) -> bigquery.Client:
    """Create BQ API Client."""
    global _bq_client
    credentials = create_credentials(key_path)
    if _bq_client is None:
        _bq_client = bigquery.Client(project=PROJECT_ID, credentials=credentials)  # noqa
    return _bq_client


def create_storage_client(key_path: Optional[str] = KEY_PATH) -> storage.Client:
    """Create GCS API Client."""
    global _storage_client
    credentials = create_credentials(key_path)
    if _storage_client is None:
        _storage_client = storage.Client(project=PROJECT_ID, credentials=credentials)
    return _storage_client


def create_credentials(key_path: Optional[str] = None) -> service_account.Credentials:
    """Create service account credentials."""
    global _credentials
    if _credentials is None:
        with warnings.catch_warnings():
            warnings.filterwarnings('ignore', message='Your application has authenticated using end user credentials')
            if key_path is not None:
                _credentials = service_account.Credentials.from_service_account_file(
                    key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
                )
            elif _credentials is None:
                _credentials, _ = google.auth.default()
    return _credentials


def write_query(sql: Union[str, Path],
                destination: str,
                clobber=False,
                **config_kw) -> QueryJob:
    """Run a query and write the result to a BigQuery table.
    :param sql: Query SQL as text or a :class:`pathlib.Path` to a SQL file.
    :param destination: Destination as ``dataset.table``.
    :param clobber: If ``True``, overwrite the destination table if it exists.
    :param config_kw: Passed to :class:`bigquery.QueryJobConfig`.
    :return: Completed QueryJob.
    :raises: :class:`google.api_core.exceptions.GoogleAPICallError` if the request is unsuccessful.
    """
    if isinstance(sql, Path):
        sql = sql.read_text()
    client = create_bq_client()
    destination_id = f'{PROJECT_ID}.{destination}'
    print(f'Writing {destination}')
    config = bigquery.QueryJobConfig(destination=destination_id,
                                     write_disposition='WRITE_TRUNCATE' if clobber else 'WRITE_EMPTY',
                                     use_legacy_sql=False,
                                     **config_kw)
    job = client.query(sql, job_config=config)
    # Wait for job to finish, or raise an error if unsuccessful
    _ = job.result()
    return job


def extract_table(table: str, destination: str):
    """Extract a BQ table to GCS as gzipped JSONL.

    :param table: Source as ``dataset.table``.
    :param destination: GCS path starting with ``gs://`` and ending with ``-*.jsonl.gz``.
    :return:
    """
    assert destination.endswith('-*.jsonl.gz')
    client = create_bq_client()
    dataset_id, table_id = table.split('.')
    dataset_ref = bigquery.DatasetReference(PROJECT_ID, dataset_id)
    table_ref = dataset_ref.table(table_id)
    job = client.extract_table(
        table_ref,
        destination,
        job_config=ExtractJobConfig(destination_format="NEWLINE_DELIMITED_JSON", compression="GZIP"),
        location="US",
    )
    # Block
    _ = job.result()
    return job


def download(bucket, prefix, output_dir, preserve_dirs=False):
    output_dir = Path(output_dir)
    assert output_dir.exists() and output_dir.is_dir()
    client = create_storage_client()
    bucket = client.get_bucket(bucket)
    assert bucket.exists()
    blobs = list(client.list_blobs(bucket, prefix=prefix))
    if not blobs:
        raise FileNotFoundError(f'No blobs in "gs://{bucket.name}" with prefix "{prefix}"')
    assert blobs
    progress = tqdm(total=len(blobs))
    for blob in blobs:
        progress.desc = blob.name
        if preserve_dirs:
            output_path = output_dir / blob.name
            output_path.parent.mkdir(parents=True, exist_ok=True)
        else:
            output_path = output_dir / Path(blob.name).name
        blob.download_to_filename(output_path)
        progress.update()


def delete_blobs(bucket: str, prefix: str) -> None:
    client = create_storage_client()
    bucket = client.get_bucket(bucket)
    blobs = list(client.list_blobs(bucket, prefix=prefix))
    for blob in blobs:
        blob.delete()
        print(f'Deleted {blob.name}')


def get_schema(dataset: str, table: str) -> List[SchemaField]:
    _client = create_bq_client()
    table_ref = _client.get_table(f'{dataset}.{table}')
    return table_ref.schema


def schema_to_dict(schema: List[SchemaField]) -> List[dict]:
    return [field.to_api_repr() for field in schema]


def dict_to_schema(schema: List[dict]) -> List[SchemaField]:
    return [SchemaField.from_api_repr(field) for field in schema]


def update_schema(dataset: str, table: str, schema: List[SchemaField]) -> Table:
    _client = create_bq_client()
    table_ref = _client.get_table(f'{dataset}.{table}')
    table_ref.schema = schema
    table_ref = _client.update_table(table_ref, ['schema'])
    print(f"Updated schema for {table}")
    return table_ref
