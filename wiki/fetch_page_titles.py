"""
Fetch Wiki page titles for as many fields as possible.

Given the English page title for a field according to MAG, or at least the name of the field in English, we look for an
English and Chinese Wikipedia page title.

Takes as input ``fields.tsv`` (see ``read_field_meta.py``) and (after first run) `manual_page_titles.tsv`.
Writes to disk ``field_pages.json`` (see ``field_page_stats.py`` and ``fetch_page_content.py``).
"""
import csv
import json
from typing import List, Optional

import requests
from cachecontrol import CacheControl
from cachecontrol.caches.file_cache import FileCache
from more_itertools import grouper
from tqdm import tqdm

API_URL = 'https://en.wikipedia.org/w/api.php'
CHUNK_SIZE = 25
SEARCH_THRESHOLD = 0

# Disk-based cache for requests
session = CacheControl(requests.Session(), cache=FileCache('.web_cache'))


def main() -> None:
    with open('data/field_pages.json', 'wt') as f, \
            open('data/not_found.txt', 'wt') as f_error, \
            open('data/search.tsv', 'wt') as f_search:
        pbar = tqdm()
        # Etiquette is to send more than one page request at a time to the API; we get back an array with a result per
        # page. Unclear exactly how big our chunks can be, but at chunk size of 25 this script takes < 10 minutes for
        # levels 0-2
        for fields in grouper(read_fields(), CHUNK_SIZE):
            # Get page titles we'll look for
            fields = update_from_search(fields)
            titles = [x['wiki_title_1'] for x in fields if (x and x != "")]
            titles_2, titles_3 = [x['wiki_title_2'] for x in fields if x and x['wiki_title_2']], \
                                          [x['wiki_title_3'] for x in fields if x and x['wiki_title_3']]
            # Get language links for all fields in the chunk at once, in a single request
            links = [get_links(titles, 1), get_links(titles_2, 2), get_links(titles_3, 3)]
            for field in fields:
                if field is None:
                    # fill value of grouper is None
                    continue
                for index, title_val in enumerate(["wiki_title_1", "wiki_title_2", "wiki_title_3"]):
                    if field[title_val] != "":
                        try:
                            if "#" in field[title_val]:
                                field.update({f"{title_val}_section": field[title_val].split("#")[1]})
                            # The return value from get_links() is a dict keyed by the wiki_title we searched on for each page
                            field.update(links[index][field[title_val].split("#")[0].replace("_", " ")])
                        except KeyError:
                            # If the wiki_title doesn't appear as a key in the result, we didn't find a corresponding page ...
                            # In that case, run a search for similar pages and grab the most relevant
                            search_result = page_search(field[title_val])
                            try:
                                # We'll have to review the search results in case the most relevant page isn't actually about
                                # the field, just mentions it in passing
                                f_search.write(f"{field[title_val]}\t{search_result['title']}\n")
                            except (KeyError, TypeError):
                                # It's unlikely we have zero search results, but in that case, write the wiki_title here
                                f_error.write(json.dumps(field) + '\n')
                    del field[title_val]
                f.write(json.dumps(field) + '\n')
                pbar.update()


def update_from_search(fields: list) -> list:
    """
    Update best-guess page titles from manually reviewed search results
    :param fields: list of fields with their information
    :return: updated fields
    """
    with open('data/manual_page_titles.tsv', 'rt') as f:
        # This file is the result of copying search.tsv to manual_page_titles.tsv, and then removing the rows that look
        # like bad search results
        manual_titles = {x[0]: x[1] for x in [row.split('\t') for row in f]}
    for field in fields:
        if field is None:
            continue
        for title_val in ["wiki_title_1", "wiki_title_2", "wiki_title_3"]:
            if field[title_val] in manual_titles:
                field[title_val] = manual_titles[field[title_val]]
    return fields


def read_fields() -> dict:
    """
    Reads all fields from the tsv
    :return: yields field rows
    """
    # Read our field metadata from the disk
    with open('data/all_fields.tsv', 'rt') as f:
        reader = csv.DictReader(f, delimiter='\t')
        for row in reader:
            row['level'] = int(row['level'])
            yield row


def get_links(titles: List[str], title_num: int, **kw):
    """Send an API request for English Wikipedia pages' langlinks: the links to corresponding pages in other languages.

    :param titles: Titles of desired pages. Etiquette is to batch these up.
    :param title_num: Counting num for indicating which title among potentially multiple titles this is
    :param kw: Continuation parameters (for pagination) passed to make_params.
    :return: A dict keyed by page title from 'titles' with values {'en_title': str, 'zh_title': str, 'page_id': int} if
    both EN and ZH page titles were found, or just 'en_title' and 'page_id' if only EN page title found.
    """
    # if none of the CHUNK_SIZE wiki title fields has a value just return an empty dict
    # this will happen sometimes for the wiki title 2 or wiki title 3 field
    if not titles:
        return {}
    r = session.get(API_URL, params=make_params(titles, **kw))
    r.raise_for_status()
    response = r.json()
    # We extract the langlinks below into this output dict
    links = {}
    # Each element in the 'pages' dict corresponds to one of the 'titles' we passed
    for page_id, page_data in response['query']['pages'].items():
        try:
            # If we get page_id and page_data, the EN page exists. Place this data in the output keyed by the EN page
            # title. It may be possible for this to be slightly different than the title in 'titles', if the API handled
            # any redirects for us. That will trigger a KeyError when we look for the result in the caller, iterating
            # 'for field in fields above' ... and then we'd resolve the difference with a page search.
            if page_id == "-1":
                raise KeyError
            links[page_data['title']] = {
                f'en_title_{title_num}': page_data['title'],
                f'page_id_{title_num}': page_id,
            }
            # # If the 'langlinks' element has data, then the ZH page exists too. It'll appear as the first element in the
            # # langlinks array because in make_params() we request only the 'zh' langlink and no more than 1 result. Then
            # # the langlink data, if available, will look like [{'lang': 'zh', '*': '企业'}]. So below yields e.g. '企业'.
            # # We store the data in the output keyed by the title we searched on.
            # links[page_data['title']]['zh_title'] = page_data['langlinks'][0]['*']
        except KeyError:
            continue
        if 'continue' in response:
            print('Continuing!')
            continued_links = get_links(titles, **response['continue'])
            links.update(continued_links)
    return links


def page_search(query: str) -> Optional[dict]:
    """Search for a page on English Wikipedia.


    :param query: Search term, which will be the name of a field.
    :return: The the top page result, if any, as a dict containing the page name and ID; else None. But there's always
    1+ result so far.

    Reference: https://www.mediawiki.org/wiki/API:Search
    """
    r = session.get(API_URL, params={
        'action': 'query',
        'format': 'json',
        'list': 'search',
        # we're just using the top result
        'srlimit': 1,
        # Include the redirect title, in case useful--but in the end we aren't using this
        'srprop': 'redirecttitle',
        'srsearch': query,
    })
    r.raise_for_status()
    response = r.json()
    try:
        # If we have 1+ search result and the API request was successful, here's where it'll be ...
        top_result = response['query']['search'][0]
        del top_result['ns']
        return top_result
    except (KeyError, IndexError):
        return None

def make_params(titles: List[str], **kw) -> dict:
    """Create query params for an API request that gets titles and page ids.

    :param titles: Titles of desired pages. Etiquette is to batch these up.
    :param kw: Continuation parameters (for pagination).
    :return: Dict of query params.

    Reference:
        - https://en.wikipedia.org/w/api.php?action=help&modules=query%2Binfo
        - https://www.mediawiki.org/wiki/API:Query#Example_4:_Continuing_queries
    """
    params = {
        'action': 'query',
        'titles': '|'.join(s.split("#")[0] for s in titles if s),
        'format': 'json',
        **kw,
    }
    return params


if __name__ == '__main__':
    main()
