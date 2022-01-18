#!/usr/bin/env bash

set -euo pipefail

python read_field_meta.py
python fetch_page_titles.py
python field_page_stats.py
python fetch_page_content.py

python embed_field_text.py \
  ../field-of-study-pipelines/assets/scientific-lit-embeddings/english/fasttext/en_merged_model_120221.bin \
  ../field-of-study-pipelines/assets/scientific-lit-embeddings/english/tfidfs/tfidf_model_en_merged_sample.pkl \
  ../field-of-study-pipelines/assets/scientific-lit-embeddings/english/tfidfs/id2word_dict_en_merged_sample.txt \
  --lang=en

python embed_field_text.py \
  ../field-of-study-pipelines/assets/scientific-lit-embeddings/chinese/fasttext/zh_merged_model_011322.bin \
  ../field-of-study-pipelines/assets/scientific-lit-embeddings/chinese/tfidfs/tfidf_model_zh_sample_011222.pkl \
  ../field-of-study-pipelines/assets/scientific-lit-embeddings/chinese/tfidfs/id2word_dict_zh_sample_011222.txt \
  --lang=zh
