"""
Download a previously constructed sample from the merged EN corpus for evaluation.
"""
from pathlib import Path

from fos.gcp import download_table, write_query

agreement_dir = Path(__file__).parent


def main():
    table = 'field_model_replication.sample_10_text'
    write_query('select fields.*, en_corpus.text '
                'from fields_of_study.field_scores fields '
                'inner join field_model_replication.sample_10 using(merged_id) '
                'inner join staging_new_fields_of_study.en_corpus using(merged_id) '
                'where not is_imputed',
                table, clobber=True)
    download_table(table, 'fields-of-study', 'model-replication/en_sample_10', agreement_dir)


if __name__ == '__main__':
    main()
