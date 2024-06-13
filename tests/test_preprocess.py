from fos.util import preprocess, TO_CLEAN_LOWER, LONE_NUMBERS


def test_replace():
    assert 'QUICK BROWN FOX?'.translate(TO_CLEAN_LOWER) == 'quick brown fox'
    assert 'A 0e!"#$%&\'()*+,\-./:;<=>?@[\\\]^_`{|}~]'.translate(TO_CLEAN_LOWER) == 'a 0e'
    assert '产业组织理论'.translate(TO_CLEAN_LOWER) == '产业组织理论'
    assert '\r\n'.translate(TO_CLEAN_LOWER) == '  '
    assert LONE_NUMBERS.sub('', 'X11 11') == 'X11 '


def test_preprocess():
    assert preprocess(' QUICK BROWN FOX?', 'en') == 'quick brown fox'
    assert preprocess('产业组织理论', 'zh') == '产业组织理论'
