import json
import tempfile
from pathlib import Path

import dataset

from fos.gcp import file_to_table, create_bq_client, download_query
from fos.util import iter_bq_extract

db = dataset.connect('sqlite:///wiki/data/wiki.db')
page_table = db['pages']
ref_table = db['refs']

client = create_bq_client()


def main(update=False):
    """
    doi pmid s2 ads issn pmc oclc arxiv
    """
    upload_references(clobber=True)
    for ref_type in ["doi", "pmid", "pmc", "s2"]:
        fetch_identifier(ref_type, clobber=True)
        for record in iter_bq_extract(f'ref-{ref_type}', 'wiki/data/corpus'):
            update = {'id': record['id'], 'db_id': record['merged_id'], f'en_text': record['text']}
            ref_table.update(update, ['id'])


def upload_references(clobber: bool = False) -> None:
    """
    Upload references to BQ for matching against merged corpus.
    :param clobber: bool indicating if we want to clobber
    :return: None
    """
    with tempfile.NamedTemporaryFile('w+t') as f:
        i = 0
        for i, record in enumerate(db.query('select * from refs where id_value is not null'), 1):
            f.write(json.dumps(record) + '\n')
        print(f'Dumped {i:,} references to tempfile')
        file_to_table(f.name, 'field_model_replication.wiki_references_temp', clobber=clobber)


def fetch_identifier(identifier: str, clobber: bool = False) -> None:
    """
    Fetch reference text based on an identifier (doi, pmid, pmc, s2)
    :param identifier: The identifier string
    :param clobber: bool indicating if we want to clobber
    :return: None
    """
    sql = Path(f'wiki/sql/{identifier}.sql').read_text()
    table = f'field_model_replication.ref_text_{identifier}'
    bucket = 'fields-of-study'
    prefix = f'model-replication/ref-{identifier}'
    download_query(sql, table, bucket, prefix, Path('wiki/data/corpus'), clobber)


if __name__ == '__main__':
    main(update=False)
