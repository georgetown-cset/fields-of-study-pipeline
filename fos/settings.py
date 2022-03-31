from pathlib import Path

LANGUAGES = ('en', 'zh')
FASTTEXT_DIM = 250

FOS_DIR = Path(__file__).parent
PIPELINES_DIR = FOS_DIR.parent
ASSETS_DIR = PIPELINES_DIR / 'assets'
CORPUS_DIR = ASSETS_DIR / 'corpus'
SQL_DIR = PIPELINES_DIR / 'sql'

QUERY_PATH = SQL_DIR / 'corpus.sql'

EN_FASTTEXT_PATH = ASSETS_DIR / 'en_fasttext.bin'
EN_TFIDF_PATH = ASSETS_DIR / 'en_tfidf.pkl'
EN_DICT_PATH = ASSETS_DIR / 'en_vocab.txt'

ZH_FASTTEXT_PATH = ASSETS_DIR / 'zh_fasttext.bin'
ZH_TFIDF_PATH = ASSETS_DIR / 'zh_tfidf.pkl'
ZH_DICT_PATH = ASSETS_DIR / 'zh_vocab.txt'

EN_ENTITY_PATH = ASSETS_DIR / 'en_entity_trie.pkl'
ZH_ENTITY_PATH = ASSETS_DIR / 'zh_entity_trie.pkl'

EN_FIELD_FASTTEXT_PATH = ASSETS_DIR / 'en_field_fasttext_similarity.pkl'
EN_FIELD_TFIDF_PATH = ASSETS_DIR / 'en_field_tfidf_similarity.pkl'
EN_FIELD_ENTITY_PATH = ASSETS_DIR / 'en_field_entity_similarity.pkl'
EN_FIELD_KEY_PATH = ASSETS_DIR / 'en_field_keys.txt'

ZH_FIELD_FASTTEXT_PATH = ASSETS_DIR / 'zh_field_vectors.pkl'
ZH_FIELD_TFIDF_PATH = ASSETS_DIR / 'zh_field_tfidf_similarity.pkl'
ZH_FIELD_ENTITY_PATH = ASSETS_DIR / 'zh_field_entity_similarity.pkl'
ZH_FIELD_KEY_PATH = ASSETS_DIR / 'zh_field_keys.txt'

# These are CSV dumps of data for the Go implementation
EN_ENTITY_CSV = ASSETS_DIR / 'en_entity_trie.csv'
EN_FIELD_FASTTEXT_CSV = ASSETS_DIR / 'en_field_fasttext_vectors.csv'
EN_FIELD_TFIDF_JSON = ASSETS_DIR / 'en_field_tfidf_vectors.json'
EN_FIELD_ENTITY_CSV = ASSETS_DIR / 'en_field_entity_vectors.csv'

# These are CSV dumps of data for the Go implementation
ZH_ENTITY_CSV = ASSETS_DIR / 'zh_entity_trie.csv'
ZH_FIELD_FASTTEXT_CSV = ASSETS_DIR / 'zh_field_fasttext_vectors.csv'
ZH_FIELD_TFIDF_JSON = ASSETS_DIR / 'zh_field_tfidf_vectors.json'
ZH_FIELD_ENTITY_CSV = ASSETS_DIR / 'zh_field_entity_vectors.csv'

# For debugging, field content ...
EN_FIELD_TEXT = ASSETS_DIR / 'en_field_text.json'
ZH_FIELD_TEXT = ASSETS_DIR / 'zh_field_text.json'

BIN_PATH = PIPELINES_DIR / 'fields'
