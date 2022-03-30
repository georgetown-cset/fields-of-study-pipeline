#!/usr/bin/env bash
# Produce original MAG field scores for the top venue papers

set -euo pipefail

WD=$PWD
LS_DIR="/home/james/dev/fields-of-study/src/Microsoft.Academic.LanguageSimilarity"
EXE_DIR="$LS_DIR/bin/Release"
MODEL_DIR="$LS_DIR/../resources"
INPUT="$WD"/ai_venue_text.jsonl
OUTPUT="$WD"/ai_venue_text_mag_scores.jsonl

cd $EXE_DIR
mono Microsoft.Academic.LanguageSimilarity.exe "$INPUT" "$OUTPUT" --model_dir=$MODEL_DIR
