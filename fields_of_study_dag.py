import json
import os

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator, BigQueryCheckOperator
from airflow.providers.google.cloud.transfers.bigquery_to_bigquery import BigQueryToBigQueryOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator
from airflow.providers.google.cloud.operators.compute import ComputeEngineStartInstanceOperator, ComputeEngineStopInstanceOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
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
    "on_failure_callback": task_fail_slack_alert
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
    gcs_folder = "new-fields-of-study"
    outputs_dir = f"{gcs_folder}/outputs"
    schema_dir = f"{gcs_folder}/schemas"
    sql_dir = f"sql/{gcs_folder}"
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
            f"rm -r fields-of-study-pipeline",
            f"gsutil cp gs://{bucket}/{production_dataset}/fields-of-study-pipeline ."
        ])
    )

    gce_instance_start >> refresh_artifacts

    # stop the instance
    gce_instance_stop = ComputeEngineStopInstanceOperator(
        project_id=project_id,
        zone=gce_zone,
        resource_id=gce_resource_id,
        task_id="stop-"+gce_resource_id
    )

    for lang in ["en", "zh"]:
        download = BashOperator(
            task_id=f"download_{lang}",
            bash_command = mk_command_seq([
                # make sure the corpus dir is clean
                f"rm -r fields-of-study-pipeline/assets/corpus/* || true",
                "cd fields-of-study-pipeline",
                f"python3 scripts/download_corpus.py {lang}"
            ])
        )

        score_corpus = BashOperator(
            task_id=f"score_corpus_{lang}",
            bash_command=mk_command_seq([
                "cd fields-of-study-pipeline",
                f"python3 scripts/score_corpus.py {lang}"
                # make sure this gcs folder is empty before upload so this task can be retried without worrying about
                # old outputs hanging around
                f"gsutil rm -r gs://{bucket}/{outputs_dir}/*",
                f"gsutil cp assets/corpus/{lang}_scores.jsonl gs://{bucket}/{outputs_dir}/"
            ])
        )

        refresh_artifacts >> download >> score_corpus >> gce_instance_stop

    # gcs to bq staging

    # checks

    # copy to production

    # populate documentation

    # backup
