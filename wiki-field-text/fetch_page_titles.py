"""
Fetch Wiki page titles for as many fields as possible.

Given the English page title for a field according to MAG, or at least the name of the field in English, we look for an
English and Chinese Wikipedia page title.

Takes as input ``fields.tsv`` (see ``read_field_meta.py``).
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


def main():
    with open('field_pages2.json', 'wt') as f, \
            open('not_found2.txt', 'wt') as f_error, \
            open('search2.tsv', 'wt') as f_search:
        pbar = tqdm()
        # Etiquette is to send more than one page request at a time to the API; we get back an array with a result per
        # page. Unclear exactly how big our chunks can be, but at chunk size of 25 this script takes < 10 minutes.
        for fields in grouper(read_fields(), CHUNK_SIZE):
            # Get page titles we'll look for
            titles = [x['wiki_title'] for x in fields if x]
            # Get language links for all fields in the chunk at once, in a single request
            links = get_links(titles)
            for field in fields:
                if field is None:
                    # fill value of grouper is None
                    continue
                try:
                    # The return value from get_links() is a dict keyed by the wiki_title we searched on for each page
                    field.update(links[field['wiki_title']])
                except KeyError:
                    # If the wiki_title doesn't appear as a key in the result, we didn't find a corresponding page ...
                    # In that case, run a search for similar pages and grab the most relevant
                    search_result = page_search(field['wiki_title'])
                    try:
                        # We'll have to review the search results in case the most relevant page isn't actually about
                        # the field, just mentions it in passing
                        f_search.write(f"{field['wiki_title']}\t{search_result['title']}\n")
                        link = get_links([search_result['title']])
                        field.update(link[search_result['title']])
                    except (KeyError, TypeError):
                        # It's unlikely we have zero search results, but in that case, write the wiki_title here
                        f_error.write(json.dumps(field) + '\n')
                f.write(json.dumps(field) + '\n')
                pbar.update()


def read_fields():
    # Read our field metadata from the disk
    with open('fields.tsv', 'rt') as f:
        reader = csv.DictReader(f, delimiter='\t')
        for row in reader:
            yield row


def get_links(titles: List[str], **kw):
    """Send an API request for English Wikipedia pages' langlinks: the links to corresponding pages in other languages.

    :param titles: Titles of desired pages. Etiquette is to batch these up.
    :param kw: Continuation parameters (for pagination) passed to make_params.
    :return: A dict keyed by page title from 'titles' with values {'en_title': str, 'zh_title': str, 'page_id': int} if
    both EN and ZH page titles were found, or just 'en_title' and 'page_id' if only EN page title found.
    """
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
            links[page_data['title']] = {
                'en_title': page_data['title'],
                'page_id': page_id
            }
            # If the 'langlinks' element has data, then the ZH page exists too. It'll appear as the first element in the
            # langlinks array because in make_params() we request only the 'zh' langlink and no more than 1 result. Then
            # the langlink data, if available, will look like [{'lang': 'zh', '*': '企业'}]. So below yields e.g. '企业'.
            # We store the data in the output keyed by the title we searched on.
            links[page_data['title']]['zh_title'] = page_data['langlinks'][0]['*']
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


def make_params(titles: List[str], **kw):
    """Create query params for an API request that gets langlinks: the links to corresponding pages in other languages.

    :param titles: Titles of desired pages. Etiquette is to batch these up.
    :param kw: Continuation parameters (for pagination).
    :return: Dict of query params.

    Reference:
        - https://en.wikipedia.org/w/api.php?action=help&modules=query%2Blanglinks
        - https://www.mediawiki.org/wiki/API:Query#Example_4:_Continuing_queries
    """
    params = {
        'action': 'query',
        'titles': '|'.join(s for s in titles if s),
        'prop': 'langlinks',
        'format': 'json',
        # only get the chinese link(s)
        'lllang': 'zh',
        'lllimit': 500,
        **kw,
    }
    return params


if __name__ == '__main__':
    main()
