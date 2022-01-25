import json
import tempfile
import time
from pathlib import Path

import dataset
import requests
from tqdm import tqdm

from fos.gcp import file_to_table, create_bq_client, download_query
from fos.util import iter_bq_extract

db = dataset.connect('sqlite:///data/wiki.db')
page_table = db['pages']
ref_table = db['refs']

client = create_bq_client()


def main(update=False):
    """
    doi pmid s2 ads issn pmc oclc arxiv
    """
    # upload_references(clobber=True)
    for lang in ['en', 'zh']:
        fetch_doi(lang, clobber=True)
        for record in iter_bq_extract(f'{lang}_ref', 'data/corpus'):
            update = {'id': record['id'], 'db_id': record['merged_id'], f'{lang}_text': record['text']}
            ref_table.update(update, ['id'])
    # fetch_s2_references(update=update)


def upload_references(clobber=False):
    """Upload references to BQ for matching against merged corpus."""
    with tempfile.NamedTemporaryFile('w+t') as f:
        i = 0
        for i, record in enumerate(db.query('select * from refs where id_value is not null'), 1):
            f.write(json.dumps(record) + '\n')
        print(f'Dumped {i:,} references to tempfile')
        file_to_table(f.name, 'field_model_replication.wiki_references', clobber=clobber)


def fetch_doi(lang='en', clobber=False):
    sql = Path('sql/doi.sql').read_text()
    if lang != 'en':
        # Hacky but in the line from doi.sql pasted below, we just need to use the en_corpus table for EN and zh_corpus
        # for ZH
        #   inner join field_model_replication.en_corpus using(merged_id)
        sql = sql.replace('en_corpus', f'{lang}_corpus')
    table = f'field_model_replication.{lang}_ref_text'
    bucket = 'fields-of-study'
    prefix = f'model-replication/{lang}_ref'
    download_query(sql, table, bucket, prefix, Path('data/corpus'), clobber)


def fetch_s2_references(update=False):
    for record in tqdm(db.query('select * from refs where id_type = "s2"', 1)):
        s2_meta = request_s2(record['id_value'])
        if record.get('db_id') and not update:
            # We've already found this article
            continue
        record['db_id'] = json.dumps(s2_meta.get('externalIds'))
        record['ref_title'] = s2_meta.get('title')
        record['abstract'] = s2_meta.get('abstract')
        ref_table.update(record, ['id'])


def request_s2(corpus_id, sleep=3.1):
    params = {
        'fields': ','.join(['externalIds', 'title', 'abstract'])
    }
    r = requests.get(f'https://api.semanticscholar.org/graph/v1/paper/CorpusId:{corpus_id}', params=params)
    r.raise_for_status()
    time.sleep(sleep)
    return r.json()


if __name__ == '__main__':
    main(update=False)
