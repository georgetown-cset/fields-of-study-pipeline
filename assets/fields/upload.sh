#!/usr/bin/env bash

set -euo pipefail
set -x

bq load \
  --replace \
  --source_format=NEWLINE_DELIMITED_JSON \
  --schema ../../schemas/field_meta.json \
  staging_fields_of_study.field_meta field_meta.jsonl

bq load \
  --replace \
  --source_format=NEWLINE_DELIMITED_JSON \
  --schema ../../schemas/field_children.json \
  staging_fields_of_study.field_children field_children.jsonl
