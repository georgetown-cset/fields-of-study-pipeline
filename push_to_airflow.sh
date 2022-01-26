gsutil cp fields_of_study_dag.py gs://us-east1-production-41653310-bucket/dags/
gsutil rm -r gs://airflow-data-exchange/new_fields_of_study/*
gsutil -m cp -r ../fields-of-study-pipeline/{assets,.dvc,.git,fos,scripts,sql,requirements.txt} gs://airflow-data-exchange/new_fields_of_study/fields-of-study-pipeline/
