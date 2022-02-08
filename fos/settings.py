from pathlib import Path

LANGUAGES = ('en', 'zh')
FASTTEXT_DIM = 250

FOS_DIR = Path(__file__).parent
PIPELINES_DIR = FOS_DIR.parent
ASSETS_DIR = PIPELINES_DIR / 'assets'
CORPUS_DIR = ASSETS_DIR / 'corpus'
EMBED_DIR = ASSETS_DIR / 'scientific-lit-embeddings'
SQL_DIR = PIPELINES_DIR / 'sql'

QUERY_PATH = SQL_DIR / 'corpus.sql'

EN_FASTTEXT_PATH = EMBED_DIR / 'english/fasttext/en_merged_model_120221.bin'
EN_TFIDF_PATH = EMBED_DIR / 'english/tfidfs/tfidf_model_en_merged_sample.pkl'
EN_DICT_PATH = EMBED_DIR / 'english/tfidfs/id2word_dict_en_merged_sample.txt'

ZH_FASTTEXT_PATH = EMBED_DIR / 'chinese/fasttext/zh_merged_model_020422_tokenized.bin'
ZH_TFIDF_PATH = EMBED_DIR / 'chinese/tfidfs/tfidf_model_zh_sample_011222.pkl'
ZH_DICT_PATH = EMBED_DIR / 'chinese/tfidfs/id2word_dict_zh_sample_011222.txt'

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
