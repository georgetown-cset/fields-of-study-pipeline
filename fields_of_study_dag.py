import json
import os

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator, BigQueryCheckOperator
from airflow.providers.google.cloud.transfers.bigquery_to_bigquery import BigQueryToBigQueryOperator
from airflow.providers.google.cloud.operators.compute import ComputeEngineStartInstanceOperator, \
    ComputeEngineStopInstanceOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.hooks.base_hook import BaseHook
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
from datetime import timedelta, datetime

from dataloader.airflow_utils.slack import task_fail_slack_alert
from dataloader.scripts.populate_documentation import update_table_descriptions


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2020, 12, 12),
    "email": [],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    #"on_failure_callback": task_fail_slack_alert
}

production_dataset = "new_fields_of_study"
staging_dataset = f"staging_{production_dataset}"


def mk_command_seq(cmds: list) -> str:
    scripts = " && ".join(cmds)
    return (f"gcloud compute ssh jm3312@{gce_resource_id} --zone {gce_zone} "
                f"--command \"{scripts}\"")


with DAG("new-fields-of-study",
            default_args=default_args,
            description="Labels our scholarly literature with fields of study",
            schedule_interval=None,
            user_defined_macros = {"staging_dataset": staging_dataset, "production_dataset": production_dataset},
            catchup=False
         ) as dag:
    slack_webhook = BaseHook.get_connection("slack")
    bucket = "airflow-data-exchange"
    outputs_dir = f"{production_dataset}/outputs"
    schema_dir = f"{production_dataset}/schemas"
    sql_dir = f"sql/{production_dataset}"
    backup_dataset = f"{production_dataset}_backups"
    project_id = "gcp-cset-projects"
    gce_zone = "us-east1-c"
    gce_resource_id = "fos-pipeline-test"
    dags_dir = os.environ.get("DAGS_FOLDER")

    # We keep script outputs in a tmp dir on gcs, so clean it out at the start of each run. We clean at
    # the start of the run so if the run fails we can examine the failed data
    clear_outputs_dir = GCSDeleteObjectsOperator(
        task_id="clear_outputs_gcs_dir",
        bucket_name=bucket,
        prefix=outputs_dir + "/"
    )

    # start the instance
    gce_instance_start = ComputeEngineStartInstanceOperator(
        project_id=project_id,
        zone=gce_zone,
        resource_id=gce_resource_id,
        task_id="start-"+gce_resource_id
    )

    # run the download script, with a new param to filter to only rows that are different from the
    # previous en/zh_corpus
    refresh_artifacts = BashOperator(
        task_id=f"refresh_artifacts",
        bash_command=mk_command_seq([
            # clear out the pipeline dir on each run and grab whatever's latest on GCS (to be updated by
            # the push_to_airflow script)
            f"rm -r fields-of-study-pipeline || true",
            f"gsutil -m cp -r gs://{bucket}/{production_dataset}/fields-of-study-pipeline .",
            "cd fields-of-study-pipeline",
            "pip install -r requirements.txt",
            "/home/jm3312/.local/bin/dvc pull",
            "cd assets/scientific-lit-embeddings/",
            "/home/jm3312/.local/bin/dvc pull"
        ])
    )

    clear_outputs_dir >> gce_instance_start >> refresh_artifacts

    prev_op = refresh_artifacts

    for lang in ["en", "zh"]:
        download = BashOperator(
            task_id=f"download_{lang}",
            bash_command = mk_command_seq([
                # make sure the corpus dir is clean
                f"rm -r fields-of-study-pipeline/assets/corpus/* || true",
                "cd fields-of-study-pipeline",
                f"PYTHONPATH=. python3 scripts/download_corpus.py {lang} --limit 100 --skip_prev --use_default_clients"
            ])
        )

        score_corpus = BashOperator(
            task_id=f"score_corpus_{lang}",
            bash_command=mk_command_seq([
                "cd fields-of-study-pipeline",
                f"PYTHONPATH=. python3 scripts/score_corpus.py {lang} --bq_format",
                f"gsutil cp assets/corpus/{lang}_scores.jsonl gs://{bucket}/{outputs_dir}/"
            ])
        )

        load_to_gcs = GCSToBigQueryOperator(
            task_id=f"import_{lang}",
            bucket=bucket,
            source_objects=[f"{outputs_dir}/{lang}_scores.jsonl"],
            schema_object=f"{schema_dir}/all_metadata_norm.json",
            destination_project_dataset_table=f"{staging_dataset}.new_{lang}",
            source_format="NEWLINE_DELIMITED_JSON",
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_TRUNCATE"
        )

        prev_op >> download >> score_corpus >> load_to_gcs
        prev_op = load_to_gcs

    # stop the instance
    gce_instance_stop = ComputeEngineStopInstanceOperator(
        project_id=project_id,
        zone=gce_zone,
        resource_id=gce_resource_id,
        task_id="stop-"+gce_resource_id
    )

    prev_op >> gce_instance_stop


    # checks
    # - before we do a write append, check there's no overlap in ids. there shouldn't be because of the way we selected
    # the data

    # copy to production

    # populate documentation

    # backup
