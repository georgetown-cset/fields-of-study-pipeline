from pathlib import Path

FASTTEXT_DIM = 250

FOS_DIR = Path(__file__).parent
PIPELINES_DIR = FOS_DIR.parent
ASSETS_DIR = PIPELINES_DIR / 'assets'
CORPUS_DIR = ASSETS_DIR / 'corpus'
EMBEDDINGS_DIR = ASSETS_DIR / 'scientific-lit-embeddings'
SQL_DIR = PIPELINES_DIR / 'sql'

QUERY_PATH = SQL_DIR / 'corpus.sql'

EN_FASTTEXT_PATH = EMBEDDINGS_DIR / 'english/fasttext/en_merged_model_120221.bin'
EN_TFIDF_PATH = EMBEDDINGS_DIR / 'english/tfidfs/tfidf_model_en_merged_sample.pkl'
EN_DICT_PATH = EMBEDDINGS_DIR / 'english/tfidfs/id2word_dict_en_merged_sample.txt'

EN_ENTITY_PATH = ASSETS_DIR / 'en_entity_trie.pkl'

EN_FIELD_FASTTEXT_PATH = ASSETS_DIR / 'en_field_fasttext_similarity.pkl'
EN_FIELD_TFIDF_PATH = ASSETS_DIR / 'en_field_tfidf_similarity.pkl'
EN_FIELD_ENTITY_PATH = ASSETS_DIR / 'en_field_entity_similarity.pkl'
EN_FIELD_KEY_PATH = ASSETS_DIR / 'en_field_keys.txt'

# These are CSV dumps of data for the Go implementation
EN_ENTITY_CSV = ASSETS_DIR / 'en_entity_trie.csv'
EN_FIELD_FASTTEXT_CSV = ASSETS_DIR / 'en_field_fasttext_vectors.csv'
EN_FIELD_TFIDF_JSON = ASSETS_DIR / 'en_field_tfidf_vectors.json'
EN_FIELD_ENTITY_CSV = ASSETS_DIR / 'en_field_entity_vectors.csv'

# For debugging, field content ...
EN_FIELD_TEXT = ASSETS_DIR / 'en_field_text.json'

BIN_PATH = PIPELINES_DIR / 'fields'
