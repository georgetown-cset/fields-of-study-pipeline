"""
Preprocess field of study metadata from MAG.

Most importantly, we select which Wiki page titles we'll try to use for each field, when retrieving text from Wikipedia.
The ``FieldOfStudyAttributes`` table has some Wikipedia URLs in it, but not for all fields in the ``FieldsOfStudy``
table. For the rest, we'll use display names from MAG.

Optionally, we restrict to fields in a level range to cut down on the time we'll later spend hitting the API
(see ``main.py``). There are just ~138K fields in levels 0-2, compared to more than 700K overall.

Takes as input ``fos.pkl.gz`` and ``fos_attr.pkl.gz``, which are from the ``fields-of-study-model`` repo.
Writes to disk ``fields.tsv``.

Requires pandas -- the pickled objects are dataframes.
"""
import gzip
import pickle
import re
from urllib.parse import urlparse, unquote

KEEP_COLS = ['level', 'display_name', 'normalized_name', 'wiki_title']


def main(max_level=1):
    fos = read_fields(max_level=max_level)
    fos[KEEP_COLS].to_csv('data/fields.tsv', index=True, sep='\t')
    print(f'Wrote {fos.shape[0]:,} rows to disk')


def read_fields(max_level=1):
    # Unpickle and wrangle the field metadata from MAG
    # this file corresponds with the FieldsOfStudy table
    fos = pickle.load(gzip.open('data/fos.pkl.gz'))
    if max_level:
        fos = fos.loc[(fos['level'] >= 0) & (fos['level'] <= max_level)].copy()
    # this file is from the FieldOfStudyAttributes table
    attr = pickle.load(gzip.open('data/fos_attr.pkl.gz'))
    # select EN wiki URLs from among the field attributes
    attr = attr.loc[(attr['type'] == 2) & (attr['value'].str.contains('en.wikipedia.org'))].copy()
    # extract just the page title
    attr['wiki_title'] = attr['value'].apply(url_to_page_id)
    assert not attr.index.duplicated().any()
    # join, where available, with fields (not all fields will have an EN wiki page title)
    fos = fos.merge(attr, how='left', on='id')
    print(f"Found wiki URLs in MAG for {(~fos['wiki_title'].isnull()).sum():,} of {fos.shape[0]:,} fields")
    # if we don't have a wiki page title, we'll use the display name
    print('Using display names for other fields')
    fos.loc[fos['wiki_title'].isnull(), 'wiki_title'] = fos.loc[fos['wiki_title'].isnull(), 'display_name']
    # we should have a wiki_title to use for each field
    assert not fos['wiki_title'].isnull().any()
    # while we're at it, make some other guarantees
    assert not fos['level'].isnull().any()
    assert not fos['level'].isnull().any()
    return fos


def url_to_page_id(url):
    # Get the path properly -- there's variation in scheme and possibly netloc
    path = urlparse(url).path
    # Remove the leading '/wiki/' from the path and replace underscores with spaces
    return re.sub(r'^/wiki/', '', unquote(path)).replace('_', ' ')


if __name__ == '__main__':
    main()
