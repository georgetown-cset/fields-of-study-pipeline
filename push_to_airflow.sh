#!/usr/bin/env bash

set -euo pipefail
set -x

AIRFLOW_PREFIX="gs://us-east1-production-cc2-202-b42a7a54-bucket/dags"
DATA_PREFIX="gs://airflow-data-exchange/fields_of_study_v2"
ROOT_PREFIX="$DATA_PREFIX/fields-of-study-pipeline"
ASSET_PREFIX="$DATA_PREFIX/fields-of-study-pipeline/assets/"

gsutil -m cp new_fields_of_study_dag.py "$AIRFLOW_PREFIX"/

gcloud storage rm --recursive $DATA_PREFIX
gcloud storage cp "schemas/*" $DATA_PREFIX/fields-of-study-pipeline/schemas/

gcloud storage rm "$AIRFLOW_PREFIX/sql/fields_of_study_v2/*"
gcloud storage cp "sql/*" $AIRFLOW_PREFIX/sql/fields_of_study_v2/

gsutil -m cp -r ./{fos,scripts,sql,requirements.txt} $ROOT_PREFIX/
gcloud storage cp "schemas/*" $AIRFLOW_PREFIX/schemas/fields_of_study_v2/
gcloud storage cp query_sequence.txt $AIRFLOW_PREFIX/sequences/fields_of_study_v2/

## Top-level assets that we need in ./assets/
gsutil -m cp "assets/*" $ASSET_PREFIX

# Field metadata we need in ./assets/fields/
gsutil cp -r assets/fields $ASSET_PREFIX

