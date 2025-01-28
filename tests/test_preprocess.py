from fos.util import preprocess, TO_CLEAN_LOWER, LONE_NUMBERS, case_field_name, clean_field_name


def test_replace():
    assert 'QUICK BROWN FOX?'.translate(TO_CLEAN_LOWER) == 'quick brown fox'
    assert 'A 0e!"#$%&\'()*+,\-./:;<=>?@[\\\]^_`{|}~]'.translate(TO_CLEAN_LOWER) == 'a 0e'
    assert '产业组织理论'.translate(TO_CLEAN_LOWER) == '产业组织理论'
    assert '\r\n'.translate(TO_CLEAN_LOWER) == '  '
    assert LONE_NUMBERS.sub('', 'X11 11') == 'X11 '


def test_preprocess():
    assert preprocess(' QUICK BROWN FOX?', 'en') == 'quick brown fox'


def test_case_field_name():
    assert case_field_name('Computer science') == 'Computer Science'
    assert case_field_name('Neuro-symbolic AI') == 'Neuro-Symbolic AI'
    assert case_field_name('text-to-speech') == 'Text-to-Speech'
    assert case_field_name('Text-To-Image Models') == 'Text-to-Image Models'
    assert case_field_name('System On A Chip') == 'System on a Chip'
    assert case_field_name('Zero-Knowledge Proofs') == 'Zero-Knowledge Proofs'
    assert case_field_name('Law and society') == 'Law and Society'
    assert case_field_name('The Law and society') == 'The Law and Society'
    assert case_field_name('GPU') == 'GPU'
    assert case_field_name('Brain-computer interfacing') == 'Brain-Computer Interfacing'
    assert case_field_name('eWLB packaging') == 'eWLB Packaging'
    assert case_field_name('System in a package') == 'System in a Package'
    assert case_field_name('Risk analysis (Engineering)') == 'Risk Analysis (Engineering)'


def test_clean_field_name():
    assert clean_field_name('Computer  science') == 'Computer science'
    assert clean_field_name(' Computer  science ') == 'Computer science'
    assert clean_field_name(' text–to–speech') == 'text-to-speech'
