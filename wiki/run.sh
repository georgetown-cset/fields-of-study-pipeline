#!/usr/bin/env bash

set -euo pipefail

python read_field_meta.py
python fetch_page_titles.py
python describe_page_titles.py
python fetch_page_content.py
python fetch_reference_text.py

python embed_field_text.py --lang=en
python embed_field_text.py --lang=zh

python embed_entities.py --lang=en
python embed_entities.py --lang=zh
