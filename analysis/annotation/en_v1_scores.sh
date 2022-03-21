#!/usr/bin/env bash
# Produce v1 field scores for the annotated sample of docs.

set -euo pipefail

ANNO_DIR=$PWD
LS_DIR="/home/james/dev/fields-of-study/src/Microsoft.Academic.LanguageSimilarity"
EXE_DIR="$LS_DIR/bin/Release"
MODEL_DIR="$LS_DIR/../resources"

cd $EXE_DIR
mono Microsoft.Academic.LanguageSimilarity.exe \
   "$ANNO_DIR"/en_preprocessed_texts.jsonl \
   "$ANNO_DIR"/en_v1_scores.jsonl \
   --id_field=merged_id \
   --model_dir=$MODEL_DIR/
