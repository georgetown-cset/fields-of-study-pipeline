#!/usr/bin/env bash
# Grant necessary scopes to pipeline service account (run once)

BUCKET="gs://airflow-data-exchange"
ACCOUNT="fields-of-study@gcp-cset-projects.iam.gserviceaccount.com"

gcloud storage buckets add-iam-policy-binding $BUCKET \
    --member="serviceAccount:$ACCOUNT" \
    --role="roles/storage.admin"

gcloud projects add-iam-policy-binding gcp-cset-projects \
    --member="serviceAccount:$ACCOUNT" \
    --role="roles/bigquery.jobUser"
