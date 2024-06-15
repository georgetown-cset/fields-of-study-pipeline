"""
Extract the text content for each field and prepare it for embedding.

This requires reading content from our SQLite database ``wiki.db``, which contains the page content retrieved via many
API requests.

Takes as input ``wiki.db``.
Updates fields in ``wiki.db`` records.
"""
import os.path
import re
from pathlib import Path
from urllib.parse import urlparse, unquote, parse_qs

import dataset
from lxml import html
from tqdm import tqdm
from typing import Optional

db = dataset.connect('sqlite:///data/wiki.db')
page_table = db['pages']
ref_table = db['refs']

CITE_BRACKETS = re.compile(r'\[\d+]')
EDITS = re.compile(r'\[(quantify|which\?|according to whom\?|citation needed|verification needed|)]|This section needs additional citations for verification|Please help improve this article by adding citations to reliable sources in this section|Unsourced material may be challenged and removed|Find sources|Learn how and when to remove this template message', re.I)
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
    # It's simpler to rebuild the refs table than
    ref_table.drop()
    for record in tqdm(page_table):
        # Record has e.g.
        # loop through our potential pages; currently no field combines more than 3 pages
        # we index from 1
        page_nums = 3
        final_text = ""
        for i in range(1, page_nums + 1):
            en_html = record.get(f"en_html_{i}")
            if en_html:
                section_title = record.get(f"wiki_title_{i}_section")
                en_text, referenced = html_to_text(en_html, section_title)
                urls = extract_reference_urls(en_html, referenced)
                add_references(record["normalized_name"], record[f"en_title_{i}"], urls)
                if en_text:
                    final_text += f" {en_text}"
        record["en_text"] = final_text
        page_table.update(record, ['id'])


def add_references(field_id: str, title: str, urls: list) -> None:
    """
    Find the associated references for each field
    :param field_id: The id of the field (the normalized name)
    :param title: The title of the field (the wiki page title)
    :param urls: All associated urls
    :return:
    """
    ref_table.delete(field=field_id)
    for url in urls:
        pub_id = extract_id(url['url'])
        ref_table.insert(
            dict(field=field_id, title=title, domain=url['domain'], url=url['url'], **pub_id))


def extract_id(url: str) -> dict:
    """
    Extract ids from urls
    less frequent:
    - osti.gov/biblio/12490813
    - dl.acm.org/citation.cfm?id=160438
    - pubs.aeaweb.org/doi/pdfplus/10.1257/0895330027102
    - www.repository.cam.ac.uk/handle/1810/195777
    :param url: url to extract ids from
    :return: dict with id type and id value
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


def remove_wiki_brackets(text: str) -> str:
    """
    Remove wiki brackets from text
    :param text: wiki text
    :return: cleaned wiki text
    """
    text = CITE_BRACKETS.sub('', text)
    text = EDITS.sub('', text)
    return text


def extract_reference_urls(content, reference_list: list = []) -> list:
    """
    Extract reference urls from html content
    :param content: html content
    :param reference_list: specific reference list. This will be filled in when we're looking at an article section
    instead of a full article, so we only pull the relevant references for that section rather than all references
    :return: list of reference domains and urls
    """
    doc = html.document_fromstring(content)
    return [{'domain': get_domain(url), 'url': url} for url in get_reference_urls(doc, reference_list)]


def get_reference_urls(element, reference_list: list):
    urls = [x for x in element.xpath(REF_XPATH)]
    if reference_list:
        parents = [x.getparent().getparent().getparent() for x in urls]
        all_reference_nums = []
        for parent in parents:
            all_reference_nums.append(handle_reference(parent))
        url_references = tuple(zip(all_reference_nums, urls))
        final_references = set()
        for reference in reference_list:
            for ref in url_references:
                if ref[0] == reference:
                    final_references.add(ref[1])
        return final_references
    return set(urls)

def handle_reference(parent):
    """
    Handle the process for finding a reference number given a reference
    We pull this out as a separate function so it can be recursive
    :param parent: The html parent
    :return: The reference number in the base cases, the function call to recurse in the iterative case
    """
    parsed = str(html.tostring(parent))
    cite = parsed.find("span")
    if -1 < cite < 10:
        reference_num = get_reference_num(parsed[cite:])
        return reference_num
    else:
        li_cite = parsed.find("li")
        if -1 < li_cite < 10:
            reference_num = get_reference_num(parsed[li_cite:])
            return reference_num
        else:
            parent = parent.getparent()
            return handle_reference(parent)

def get_reference_num(reference_text: str) -> Optional[int]:
    """
    Find the exact reference number given the correct place to look for it
    :param reference_text: The text containing the reference number
    :return: The reference number
    """
    id_field = reference_text.find("id")
    cite = reference_text[id_field:]
    get_id = cite.split('"')
    if len(get_id) > 0:
        id_val = get_id[1]
        references = id_val.split("-")
        if len(references) > 0:
            reference_num = references[-1]
    try:
        reference_num = int(reference_num)
    except:
        return None
    return reference_num

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


def get_domain(url: str) -> str:
    """
    Get the domain of a URL
    :param url: str
    :return:
    """
    return WWW.sub('', urlparse(url).netloc)

def clean_references(referenced: set) -> list:
    """
    Clean the set of references (which is used when only dealing with wikipedia sections
    instead of whole pages) for later use
    :param referenced: set of references
    :return: list of reference integers
    """
    cleaned = []
    for reference in referenced:
        reference = reference.strip("[").strip("]")
        try:
            cleaned.append(int(reference))
        except:
            pass
    return cleaned


def html_to_text(content, section: str) -> tuple:
    """Extract page text its HTML.

    :param content: HTML content.
    :param section: Specific section to extract from, if we're only looking at a section instead of a whole page
    """
    doc = html.document_fromstring(content)
    if section:
        section_field = doc.xpath(f'//h2[@id="{section}"]')
        if not section_field:
            section_field = doc.xpath(f'//h3[@id="{section}"]')
        if section == "(Top)":
            section_field = doc.xpath(f'//section[@data-mw-section-id="0"]')
        for element in section_field:
            unwanted = element.xpath('//script|//style')
            for u in unwanted:
                u.drop_tree()
            if section == "(Top)":
                text = element.text_content()
            else:
                text = element.getparent().text_content()
            referenced = set(CITE_BRACKETS.findall(text))
            text = remove_wiki_brackets(text.strip())
            if section == "(Top)":
                print(text)
            return text, clean_references(referenced)
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
    return text, []


if __name__ == '__main__':
    main()
