gsutil cp new_fields_of_study_dag.py gs://us-east1-production-cc2-202-b42a7a54-bucket/dags/
gsutil rm -r gs://airflow-data-exchange/fields_of_study_v2/*
gsutil -m cp -r ../fields-of-study-pipeline/{assets,.dvc,.git,fos,scripts,sql,requirements.txt} gs://airflow-data-exchange/fields_of_study_v2/fields-of-study-pipeline/
gsutil rm gs://us-east1-production-cc2-202-b42a7a54-bucket/dags/sql/fields_of_study_v2/*
gsutil cp sql/* gs://us-east1-production-cc2-202-b42a7a54-bucket/dags/sql/fields_of_study_v2/
gsutil cp schemas/* gs://airflow-data-exchange/fields_of_study_v2/schemas/
gsutil cp schemas/* gs://us-east1-production-cc2-202-b42a7a54-bucket/dags/schemas/fields_of_study_v2/
gsutil cp query_sequence.txt gs://us-east1-production-cc2-202-b42a7a54-bucket/dags/sequences/fields_of_study_v2/
