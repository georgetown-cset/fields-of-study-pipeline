"""
Print quick summaries of output from ``fetch_page_titles.py``.
"""
import pandas as pd


def main():
    # Read input to fetch_page_titles
    tsv = pd.read_csv('data/all_fields.tsv', delimiter='\t')
    # Read output from fetch_page_titles
    pages = pd.read_json('data/field_pages.json', lines=True)

    # Ideally all the fields in the input also appear in the output, whether or not we found wiki page titles for them
    missing = list(set(tsv['normalized_name']) - set(pages['normalized_name']))
    print(f'{len(missing):,} pages missing from output:')
    print(tsv.set_index('normalized_name').loc[missing, 'wiki_title_1'])

    # We expect far less coverage of fields in Chinese wikipedia than in English wikipedia---check how much
    print('\nCoverage by level (n second titles, n primary EN titles):')
    # (19, 19) means we have 19 ZH and 19 EN wiki pages at level 0
    print(pages.groupby('level').agg({'en_title_2': lambda x: ((~x.isna()).sum(), len(x))}))


if __name__ == '__main__':
    main()
