"""
Fetch English and Chinese Wikipedia page content.

We read from the disk (``field_pages.json``) the page titles (if available) for each field, identified in the previous
step (``fetch_page_titles.py``). Using the Mediawiki API, we retrieve their content. This requires a lot of API calls,
one per field, and may periodically fail. For persistence (resume after failure), we use SQLite. (Reliable and
straightforward.) Before each API request, we check whether we already have the response in the DB, and if so skip. The
database 'id' is MAG's int field ID.

Takes as input ``field_pages.json`` (see ``fetch_page_titles.py``).
Writes to ``wiki.db``.
"""
import argparse
import json
from typing import Optional

import dataset
import wptools
from tqdm import tqdm

db = dataset.connect('sqlite:///data/wiki.db')
table = db['pages']

# Enforce uniqueness on display name in the table of page content
db.query("""create unique index if not exists display_name on pages(display_name);""")

def main(upsert=False):
    for field, existing in tqdm(read_field_meta(upsert=upsert)):
        field = get_wiki_content(field)
        write_record(field, existing=existing)


def read_field_meta(upsert: bool = False) -> tuple:
    """Iterate over field records, which contain the field ID, name, and corresponding Wikipedia titles.

    :param upsert: If true, yield fields with complete data already in the database; otherwise not.
    :return: Field record, and whether corresponding content is already in the DB.

    Example output::

        {
          "id": "58166",   <-- this is the FieldOfStudyId / field_id
          "level": 2,
          "display_name": "Fuzzy logic",
          "normalized_name": "fuzzy logic",
          "en_title": "Fuzzy logic",
          "page_id": "49180",
          "zh_title": "\u6a21\u7cca\u903b\u8f91"
        }

    First five elements above are from MAG metadata. Presence of "en_title" and "page_id" means we found an EN Wikipedia
    page; similarly "zh_title".
    """
    with open('data/field_pages.json', 'rt') as f:
        for line in f:
            record = json.loads(line)
            # Ensure no downstream duplicates via whitespace in names
            record['normalized_name'] = record['normalized_name'].strip()
            record['display_name'] = record['display_name'].strip()
            # Check whether complete field data is already in the database
            existing = table.find_one(normalized_name=record['normalized_name'].strip())
            if existing is not None:
                # We don't have a page title so there's nothing to do, or we do but already have the page HTML:
                en_done = [f"en_title_{i}" not in existing or f"en_html_{i}" in existing for i in range(1, 4)]
                # zh_done = 'zh_title' not in existing or 'zh_html' in existing
                # If we have all possible HTML and we aren't trying to update it, continue
                if en_done[0] and en_done[1] and en_done[2] and not upsert:
                    continue
            yield record, existing is not None


def get_wiki_content(field: dict) -> dict:
    """Retrieve EN and ZH (if available) wiki content for a page, given its EN and ZH (if available) page titles.

    :param field: Field record.
    :return: Field record, updated to include keys 'en_html', 'zh_html' (if available), 'en_text', and 'zh_text'
    (if available).
    """
    for index, title in enumerate(["en_title_1", "en_title_2", "en_title_3"]):
        if title in field:
            # We have an EN wiki page title for the field
            en_html = get_page_html(field[title], 'en')
            if en_html:
                field[f"en_html_{index + 1}"] = en_html
    return field


def write_record(record: dict, existing: bool = False) -> None:
    """Add/update a record to the database.

    :param record: Field record.
    :param existing: If true, update instead of insert.

    Reference:
        - https://dataset.readthedocs.io/en/latest/api.html#dataset.Table.insert
        - https://dataset.readthedocs.io/en/latest/api.html#dataset.Table.update
    """
    # We use as the SQLite id the normalized name
    if existing:
        table.update(record, ["normalized_name"])
    else:
        table.insert(record)


def get_page_html(title: str, lang: str = 'en') -> Optional[str]:
    """Request HTML for a given page from the Mediawiki API.

    :param title: Page title.
    :param lang: Wikipedia language code. But not really "which language do you want the page to be in?"; rather "in
    which Wikipedia project should we look for this page title?" en.wikipedia.org, zh.wikipedia.org, etc.

    Reference:
        - https://github.com/siznax/wptools/wiki/Examples#get-page-html
        - https://github.com/siznax/wptools/wiki/Language-Codes
    """
    try:
        page = wptools.page(title, lang=lang, silent=True)
        page.get_restbase(f'/page/html/')
    except LookupError:
        return None
    if 'html' not in page.data:
        # Unsure why this can happen
        print(f'No html data for {title}')
        return None
    return page.data['html']


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--upsert', action='store_true')
    args = parser.parse_args()
    main(upsert=args.upsert)
