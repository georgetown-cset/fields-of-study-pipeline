#!/usr/bin/env bash

set -euo pipefail
set -x

gcloud storage rsync gs://airflow-data-exchange/fields_of_study_v2/fields-of-study-pipeline . \
  --recursive

rm -rf assets/corpus || true
mkdir assets/corpus

mkdir -p ~/miniconda3
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda3/miniconda.sh
bash ~/miniconda3/miniconda.sh -b -u -p ~/miniconda3
rm ~/miniconda3/miniconda.sh
source ~/miniconda3/bin/activate
conda init --all

conda create -n fos python=3.8 -y
conda activate fos

python3 -m pip install -r requirements.txt
