"""
Extract the text content for each field and prepare it for embedding.

This requires reading content from our SQLite database ``wiki.db``, which contains the page content retrieved via many
API requests.

Takes as input ``wiki.db``.
Writes to disk ``field_content.json`` and ``field_text.tsv``.
"""
import json
import re

import dataset
from lxml import html
from tqdm import tqdm

CITE_BRACKETS = re.compile(r'\[\d+]')
RE_LINEBREAK = re.compile(r"(\r\n|[\n\v])+")
RE_NONBREAKING_SPACE = re.compile(r"[^\S\n\v]+")
RE_ZWSP = re.compile(r"[\u200B\u2060\uFEFF]+")

db = dataset.connect('sqlite:///wiki.db')
table = db['pages']


def main():
    for field in tqdm(table):
        record = json.loads(field['json'])
        # Record json has e.g.
        # 'id', 'level', 'display_name', ..., 'en_title', 'zh_title', 'html_en', 'text_en', 'html_zh', 'text_zh'
        en_html = record.get('html_en')
        if en_html:
            en_text = html_to_text(en_html)
            # FIXME: we have mediawiki garbage in here like
            # {\\displaystyle z\\in \\mathbb {C} \\backslash \\{0\\}
            en_text = remove_cite_brackets(en_text)
            en_text = normalize_whitespace(en_text)
            # TODO: add ref content to content
            # en_refs = extract_refs(en_html)
            write_text(record['id'], en_text, 'en')
        zh_html = record.get('html_zh')
        if zh_html:
            zh_text = html_to_text(zh_html)
            zh_text = normalize_whitespace(zh_text)
            # zh_refs = extract_refs(zh_html)
            write_text(record['id'], zh_text, 'zh')


def write_text(field_id, text, lang):
    with open(f"{lang}/{field_id}.txt", 'wt') as f:
        f.write(text)


def normalize_whitespace(text):
    # source: https://github.com/chartbeat-labs/textacy/blob/main/src/textacy/preprocessing/resources.py
    text = RE_ZWSP.sub("", text)
    text = RE_LINEBREAK.sub(r"\n", text)
    text = RE_NONBREAKING_SPACE.sub(" ", text)
    return text.strip()


def remove_cite_brackets(text):
    return CITE_BRACKETS.sub('', text)


def extract_refs(content):
    # Extract reference URLs from page HTML content
    doc = html.document_fromstring(content)
    return doc.xpath(f"//span[contains(@class, 'reference-text')]//a/@href")
    # cites = []
    # # CITE_DOMAINS = [
    # #     'acm.org',
    # #     'citeseerx.ist.psu.edu',
    # #     'doi.org',
    # #     'nih.gov',
    # #     'semanticscholar.org',
    # #     'worldcat.org',
    # # ]
    # # for domain in CITE_DOMAINS:
    # #     # for url in doc.xpath(f"//span[contains(@class, 'reference-text')]//a[contains(@href,'{domain}')]/@href"):
    # for url in doc.xpath(f"//span[contains(@class, 'reference-text')]//a/@href"):
    #     # parsed_url = urlparse(url)
    #     # cites[domain].add(parsed_url.path)
    #     cites.append(url)
    # return cites


def html_to_text(content) -> str:
    """Extract page text its HTML.

    :param content: HTML content.
    """
    doc = html.document_fromstring(content)
    # All our text should be in paragraphs; this restricts the text to the article text, excluding the infobox etc.
    paragraphs = doc.xpath('//p')
    return '\n'.join(p.text_content() for p in paragraphs)


def test_cite_bracket_pattern():
    assert CITE_BRACKETS.sub('', "theory.[28]\n") == 'theory.\n'
    assert CITE_BRACKETS.sub('', "theory.[28][29]\n") == 'theory.\n'


if __name__ == '__main__':
    main()
