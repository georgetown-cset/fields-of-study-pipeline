"""
Extract the text content for each field and prepare it for embedding.

This requires reading content from our SQLite database ``wiki.db``, which contains the page content retrieved via many
API requests.

Takes as input ``wiki.db``.
Writes to disk ``field_content.json`` and ``field_text.tsv``.
"""
import os.path
import re
from pathlib import Path
from urllib.parse import urlparse, unquote, parse_qs

import dataset
from lxml import html
from tqdm import tqdm

db = dataset.connect('sqlite:///data/wiki.db')
page_table = db['pages']
ref_table = db['refs']

CITE_BRACKETS = re.compile(r'\[\d+]')
EDITS = re.compile(r'\[(quantify|which\?|according to whom\?|citation needed|verification needed)]', re.I)
SECTIONS = ['See_also', 'Bibliography', 'External_links']
CITE_DOMAINS = [
    'acm.org',
    'citeseerx.ist.psu.edu',
    'doi.org',
    'nih.gov',
    'semanticscholar.org',
    'worldcat.org',
]
REF_XPATH = "//span[contains(@class, 'reference-text')]//a[not(starts-with(@href, './'))]/@href"
WWW = re.compile(r'^www\.')


def main():
    ref_table.drop()
    for record in tqdm(page_table):
        # Record has e.g.
        en_html = record.get('en_html')
        if en_html:
            en_text = html_to_text(en_html)
            # TODO: add ref content to content
            urls = extract_reference_urls(en_html)
            add_references(record['id'], record['en_title'], urls)
            record['en_text'] = en_text
            page_table.update(record, ['id'])
        zh_html = record.get('zh_html')
        if zh_html:
            zh_text = html_to_text(zh_html)
            record['zh_text'] = zh_text
            page_table.update(record, ['id'])


def add_references(field_id, title, urls):
    ref_table.delete(field=field_id)
    for url in urls:
        pub_id = extract_id(url['url'])
        ref_table.insert(
            dict(field=field_id, title=title, domain=url['domain'], url=url['url'], **pub_id))


def extract_id(url):
    """

    less frequent:
    - osti.gov/biblio/12490813
    - dl.acm.org/citation.cfm?id=160438
    - pubs.aeaweb.org/doi/pdfplus/10.1257/0895330027102
    - www.repository.cam.ac.uk/handle/1810/195777
    """
    parsed_url = urlparse(url)
    domain = get_domain(url)
    path = re.sub('^/', '', parsed_url.path)
    if domain == 'api.semanticscholar.org' and path.startswith('CorpusID:'):
        return {'id_type': 's2', 'id_value': path.replace('CorpusID:', '')}
    elif domain == 'pubmed.ncbi.nlm.nih.gov':
        return {'id_type': 'pmid', 'id_value': Path(path).name}
    elif domain == 'ncbi.nlm.nih.gov' and path.startswith('pmc'):
        return {'id_type': 'pmc', 'id_value': Path(path).name}
    elif domain in ['doi.org', 'dx.doi.org', 'hdl.handle.net', 'doi.wiley.com', 'link.springer.com']:
        return {'id_type': 'doi', 'id_value': unquote(path)}
    elif domain == 'worldcat.org':
        if 'oclc' in path:
            id_type = 'oclc'
        elif 'issn' in path:
            id_type = 'issn'
        else:
            return {}
        return {'id_type': id_type, 'id_value': Path(path).name}
    elif domain == 'ui.adsabs.harvard.edu':
        return {'id_type': 'ads', 'id_value': Path(path).name}
    elif domain == 'jstor.org':
        parents, name = os.path.split(path)
        if '/' in parents:
            _, parent = os.path.split(parents)
            return {'id_type': 'doi', 'id_value': parent + '/' + name}
        else:
            return {'id_type': 'doi', 'id_value': '10.2307/' + name}
    elif domain == 'arxiv.org':
        return {'id_type': 'arxiv', 'id_value': re.sub('^abs/', '', path)}
    elif domain in ['citeseerx.ist.psu.edu', 'aeaweb.org']:
        params = parse_qs(parsed_url.query)
        if params:
            try:
                return {'id_type': 'doi', 'id_value': params['doi'][0]}
            except (KeyError, IndexError):
                pass
    return {}


def remove_wiki_brackets(text):
    text = CITE_BRACKETS.sub('', text)
    text = EDITS.sub('', text)
    return text


def extract_reference_urls(content):
    doc = html.document_fromstring(content)
    return [{'domain': get_domain(url), 'url': url} for url in get_reference_urls(doc)]


def get_reference_urls(element):
    return set([x for x in element.xpath(REF_XPATH)])


def extract_cites(content):
    """Extract reference metadata from page HTML content.

    Not using this right now but could--worthwhile if we aren't finding URLs
    """
    cite_tags = []
    doc = html.document_fromstring(content)
    for i, cite in enumerate(doc.xpath(f"//span[contains(@class, 'reference-text')]//cite")):
        meta = {
            'classes': ' '.join([x for x in cite.get('class').split() if x not in ('citation', 'cs1')]),
            'text': cite.text_content().strip(),
            'ids': {domain: cite.xpath(f"//a[contains(@href,'{domain}')]/@href") for domain in CITE_DOMAINS},
            'urls': get_reference_urls(cite),
        }
        meta['ids']['isbn'] = set([x.text_content() for x in cite.xpath(f"//bdi")])
        cite_tags.append(meta)
    return cite_tags


def get_domain(url):
    return WWW.sub('', urlparse(url).netloc)


def html_to_text(content) -> str:
    """Extract page text its HTML.

    :param content: HTML content.
    """
    doc = html.document_fromstring(content)
    # All our text should be in paragraphs; this restricts the text to the article text, excluding the infobox etc.
    for tag in ['math', 'ref', 'nowiki', 'cite']:
        for element in doc.xpath(f'//{tag}'):
            element.getparent().remove(element)
    for element in doc.xpath('//span[contains(@class, "mw-reflink-text")]'):
        element.getparent().remove(element)
    for section in SECTIONS:
        for element in doc.xpath(f'//h2[@id="{section}"]'):
            element.getparent().remove(element)
    paragraphs = doc.xpath('//p')
    text = '\n'.join(p.text_content() for p in paragraphs).strip()
    text = remove_wiki_brackets(text)
    return text


if __name__ == '__main__':
    main()
