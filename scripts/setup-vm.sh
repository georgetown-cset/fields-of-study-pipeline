#!/usr/bin/env bash

set -euo pipefail
set -x

gcloud storage rsync gs://airflow-data-exchange/fields_of_study_v2/fields-of-study-pipeline . \
  --recursive

rm -rf assets/corpus || true
mkdir assets/corpus

export PATH="/opt/conda/bin:$PATH"
conda create -n fos python=3.8 -y

conda init bash
source activate base
conda activate fos

python3 -m pip install -r requirements.txt
