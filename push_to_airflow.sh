AIRFLOW_PREFIX="gs://us-east1-production-cc2-202-b42a7a54-bucket/dags"
DATA_PREFIX="gs://airflow-data-exchange/fields_of_study_v2"

gsutil -m cp new_fields_of_study_dag.py $AIRFLOW_PREFIX/
gsutil -m rm -r $DATA_PREFIX/*
gsutil -m cp -r ./{fos,scripts,sql,requirements.txt} $DATA_PREFIX/fields-of-study-pipeline/
gsutil -m cp schemas/* $DATA_PREFIX/fields-of-study-pipeline/schemas/

# Files directly under the assets directory
gsutil -m cp assets/* $DATA_PREFIX/fields-of-study-pipeline/assets/
# And also those in assets/fields, but not e.g. assets/corpus etc
gsutil -m cp -r assets/{fields,scientific-lit-embeddings} $DATA_PREFIX/fields-of-study-pipeline/assets/

# We need the dvc config so we can pull from the GCS remote
gsutil -m cp -r ../fields-of-study-pipeline/.dvc/config $DATA_PREFIX/fields-of-study-pipeline/.dvc/config

gsutil -m rm $AIRFLOW_PREFIX/sql/fields_of_study_v2/*
gsutil -m cp sql/* $AIRFLOW_PREFIX/sql/fields_of_study_v2/

gsutil -m cp schemas/* $AIRFLOW_PREFIX/schemas/fields_of_study_v2/
gsutil -m cp query_sequence.txt $AIRFLOW_PREFIX/sequences/fields_of_study_v2/
