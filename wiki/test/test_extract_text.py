from wiki.embed_field_text import CITE_BRACKETS
from wiki.extract_text import remove_wiki_brackets, extract_id


def test_cite_bracket_pattern():
    assert CITE_BRACKETS.sub('', "theory.[28]\n") == 'theory.\n'
    assert CITE_BRACKETS.sub('', "theory.[28][29]\n") == 'theory.\n'


def test_remove_wiki_brackets():
    assert remove_wiki_brackets('scales.[citation needed] Whereas') == 'interconnected scales. Whereas'
    assert remove_wiki_brackets('scales.[12] ') == 'interconnected scales. Whereas'


def test_extract_wiley_doi():
    assert extract_id('http://doi.wiley.com/10.1111/j.1365-2648.2006.04156.x') == {
        'id_type': 'doi', 'id_value': '10.1111/j.1365-2648.2006.04156.x'
    }


def test_extract_citeseer():
    assert extract_id('https://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.611.8411') == {
        'id_type': 'doi', 'id_value': '10.1.1.611.8411'
    }


def test_extract_arxiv():
    assert extract_id('https://arxiv.org/abs/1602.02410') == {
        'id_type': 'arxiv', 'id_value': '1602.02410'
    }
    assert extract_id('https://arxiv.org/abs/astro-ph/9904251') == {
        'id_type': 'arxiv', 'id_value': 'astro-ph/9904251'
    }


def test_extract_ads():
    assert extract_id('https://ui.adsabs.harvard.edu/abs/2017NatSR...741621S') == {
        'id_type': 'ads', 'id_value': '2017NatSR...741621S'
    }


def test_extract_jstor():
    assert extract_id('https://daily.jstor.org/virtual-roundtable-on-the-ego-and-the-id/') == {}
    assert extract_id('http://www.jstor.org/stable/2139299?origin=crossref') == {
        'id_type': 'doi', 'id_value': '10.2307/2139299'
    }
    assert extract_id('https://www.jstor.org/stable/2119315') == {
        'id_type': 'doi', 'id_value': '10.2307/2119315'
    }
    assert extract_id('https://www.jstor.org/stable/10.1525/rh.2005.23.4.303') == {
        'id_type': 'doi', 'id_value': '10.1525/rh.2005.23.4.303'
    }


def test_extract_worldcat():
    assert extract_id('https://www.worldcat.org/oclc/1104810110') == {
        'id_type': 'oclc', 'id_value': '1104810110'
    }
    assert extract_id('https://www.worldcat.org/issn/0167-4366') == {
        'id_type': 'issn', 'id_value': '0167-4366'
    }


def test_extract_doi():
    assert extract_id('//doi.org/10.1007%2Fs00190-019-01307-0') == {
        'id_type': 'doi', 'id_value': '10.1007/s00190-019-01307-0'
    }


def test_extract_s2():
    assert extract_id('https://api.semanticscholar.org/CorpusID:219470398') == {
        'id_type': 's2', 'id_value': '219470398'
    }
    assert extract_id('https://semanticscholar.org/paper/bc20491c2def35375c65db180b192a10c745c3ba') == {}


def test_extract_pmc():
    assert extract_id('https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3461123') == {
        'id_type': 'pmc', 'id_value': 'PMC3461123'
    }


def test_extract_pmid():
    assert extract_id('https://pubmed.ncbi.nlm.nih.gov/25643484') == {
        'id_type': 'pmid', 'id_value': '25643484'
    }
