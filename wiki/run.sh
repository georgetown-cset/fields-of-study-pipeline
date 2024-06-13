#!/usr/bin/env bash

set -euo pipefail

python read_field_meta.py
python edit_field_meta.py
python fetch_page_titles.py
python describe_page_titles.py
python fetch_page_content.py
python extract_text.py
python fetch_reference_text.py
python describe_text_coverage.py

python embed_field_text.py --lang=en

python embed_entities.py --lang=en
