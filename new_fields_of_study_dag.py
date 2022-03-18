"""
Updates the fields of study predictions. To only run on new records or records with title/abstract text that has
changed since the last run, trigger this dag with no parameters. To force the dag to rerun on everything,
trigger the dag with the configuration {"rerun": true}
"""

import json
import os

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator, BigQueryCheckOperator
from airflow.providers.google.cloud.transfers.bigquery_to_bigquery import BigQueryToBigQueryOperator
from airflow.providers.google.cloud.operators.compute import ComputeEngineStartInstanceOperator, \
    ComputeEngineStopInstanceOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
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
    "start_date": datetime(2022, 1, 27),
    "email": [],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": task_fail_slack_alert
}

production_dataset = "fields_of_study_v2"
staging_dataset = f"staging_{production_dataset}"


def mk_command_seq(cmds: list) -> str:
    scripts = " && ".join(cmds)
    return (f"gcloud compute ssh jm3312@{gce_resource_id} --zone {gce_zone} "
                f"--command \"{scripts}\"")


with DAG("new_fields_of_study",
            default_args=default_args,
            description="Labels our scholarly literature with fields of study",
            schedule_interval=None,
            user_defined_macros = {"staging_dataset": staging_dataset, "production_dataset": production_dataset},
            catchup=False
         ) as dag:
    slack_webhook = BaseHook.get_connection("slack")
    bucket = "airflow-data-exchange"
    tmp_dir = f"{production_dataset}/tmp"
    outputs_dir = f"{tmp_dir}/outputs"
    schema_dir = f"{production_dataset}/schemas"
    sql_dir = f"sql/{production_dataset}"
    backups_dataset = f"{production_dataset}_backups"
    project_id = "gcp-cset-projects"
    gce_zone = "us-east1-c"
    gce_resource_id = "fos-runner"
    dags_dir = os.environ.get("DAGS_FOLDER")

    # We keep script inputs and outputs in a tmp dir on gcs, so clean it out at the start of each run. We clean at
    # the start of the run so if the run fails we can examine the failed data
    clear_tmp_dir = GCSDeleteObjectsOperator(
        task_id="clear_tmp_gcs_dir",
        bucket_name=bucket,
        prefix=tmp_dir + "/"
    )

    # start the instance where we'll run the download and scoring scripts
    gce_instance_start = ComputeEngineStartInstanceOperator(
        project_id=project_id,
        zone=gce_zone,
        resource_id=gce_resource_id,
        task_id="start-"+gce_resource_id
    )

    # clear out the directory of code and dvc artifacts on each run and grab whatever's
    # latest on GCS (to be updated by the push_to_airflow script)
    refresh_artifacts = BashOperator(
        task_id=f"refresh_artifacts",
        bash_command=mk_command_seq([
            "cd /mnt/disks/data",
            f"rm -r fields-of-study-pipeline || true",
            f"gsutil -m cp -r gs://{bucket}/{production_dataset}/fields-of-study-pipeline .",
            "cd fields-of-study-pipeline",
            "pip install -r requirements.txt",
            "/home/jm3312/.local/bin/dvc pull",
            "cd assets/scientific-lit-embeddings/",
            "/home/jm3312/.local/bin/dvc pull"
        ])
    )

    clear_tmp_dir >> gce_instance_start >> refresh_artifacts

    prev_op = refresh_artifacts

    languages = ["en", "zh"]
    for lang in languages:
        # run the download script; filter inputs to only "changed" rows if the user did not pass the "rerun" param
        # through the dagrun config
        download = BashOperator(
            task_id=f"download_{lang}",
            bash_command = mk_command_seq([
                "cd /mnt/disks/data",
                # make sure the corpus dir is clean
                f"rm -r fields-of-study-pipeline/assets/corpus/* || true",
                "cd fields-of-study-pipeline",
                (f"PYTHONPATH=. python3 scripts/download_corpus.py {lang} "
                 "{{'' if dag_run and dag_run.conf.get('rerun', False) else '--skip_prev'}} "
                    f"--use_default_clients --bq_dest {staging_dataset} --extract_bucket {bucket} "
                    f"--extract_prefix {tmp_dir}/inputs/{lang}_corpus-")
            ])
        )

        score_corpus = BashOperator(
            task_id=f"score_corpus_{lang}",
            bash_command=mk_command_seq([
                "cd /mnt/disks/data/fields-of-study-pipeline",
                f"PYTHONPATH=. python3 scripts/batch_score_corpus.py {lang} --limit 0",
                f"gsutil cp assets/corpus/{lang}_scores.jsonl gs://{bucket}/{outputs_dir}/"
            ])
        )

        load_to_gcs = GCSToBigQueryOperator(
            task_id=f"import_{lang}",
            bucket=bucket,
            source_objects=[f"{outputs_dir}/{lang}_scores.jsonl"],
            destination_project_dataset_table=f"{staging_dataset}.new_{lang}",
            source_format="NEWLINE_DELIMITED_JSON",
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_TRUNCATE",
            autodetect=True
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
    prev_op = gce_instance_stop

    # Run the downstream queries in the order they appear in query_sequence.txt
    with open(f"{dags_dir}/sequences/{production_dataset}/query_sequence.txt") as f:
        for table_name in f:
            table_name = table_name.strip()
            if not table_name:
                continue
            query = BigQueryInsertJobOperator(
                task_id=f"run_{table_name}",
                configuration={
                    "query": {
                        "query": "{% include '" + f"{sql_dir}/{table_name}.sql" + "' %}",
                        "useLegacySql": False,
                        "destinationTable": {
                            "projectId": project_id,
                            "datasetId": staging_dataset,
                            "tableId": table_name
                        },
                        "allowLargeResults": True,
                        "createDisposition": "CREATE_IF_NEEDED",
                        "writeDisposition": "WRITE_TRUNCATE"
                    }
                }
            )
            prev_op >> query
            prev_op = query

    wait_for_checks = DummyOperator(task_id="wait_for_checks")

    for query in os.listdir(f"{os.environ.get('DAGS_FOLDER')}/{sql_dir}"):
        if not query.startswith("check_"):
            continue
        check = BigQueryCheckOperator(
            task_id=query.replace(".sql", ""),
            sql=f"{sql_dir}/{query}",
            params={
                "dataset": staging_dataset
            },
            use_legacy_sql=False
        )
        prev_op >> check >> wait_for_checks

    wait_for_backup = DummyOperator(task_id="wait_for_backup")

    curr_date = datetime.now().strftime('%Y%m%d')
    # copy to production, populate table descriptions, backup tables
    with open(f"{os.environ.get('DAGS_FOLDER')}/schemas/{production_dataset}/tables.json") as f:
        table_desc = json.loads(f.read())
    for table in ["field_scores", "field_meta", "field_children", "top_fields"]:
        prod_table_name = f"{production_dataset}.{table}"
        table_copy = BigQueryToBigQueryOperator(
            task_id=f"copy_{table}_to_production",
            source_project_dataset_tables=[f"{staging_dataset}.{table}"],
            destination_project_dataset_table=prod_table_name,
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_TRUNCATE"
        )
        pop_descriptions = PythonOperator(
            task_id="populate_column_documentation_for_" + table,
            op_kwargs={
                "input_schema": f"{os.environ.get('DAGS_FOLDER')}/schemas/{production_dataset}/{table}.json",
                "table_name": prod_table_name,
                "table_description": table_desc[prod_table_name]
            },
            python_callable=update_table_descriptions
        )
        table_backup = BigQueryToBigQueryOperator(
            task_id=f"back_up_{table}",
            source_project_dataset_tables=[f"{staging_dataset}.{table}"],
            destination_project_dataset_table=f"{backups_dataset}.{table}_{curr_date}",
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_TRUNCATE"
        )

        wait_for_checks >> table_copy >> pop_descriptions >> table_backup >> wait_for_backup

    success_alert = SlackAPIPostOperator(
        task_id="post_success",
        token=slack_webhook.password,
        text="(new!) fields of study update succeeded!",
        channel=slack_webhook.login,
        username="airflow"
    )

    # as a final step before posting success, update the prev_{en,zh}_corpus tables so we'll know what text we used
    # on previous runs
    for lang in languages:
        copy_corpus = BigQueryInsertJobOperator(
                task_id=f"copy_{lang}_corpus",
                configuration={
                    "query": {
                        "query": (f"select * from {staging_dataset}.{lang}_corpus "
                                  f"union all "
                                  f"(select * from {staging_dataset}.prev_{lang}_corpus "
                                  f"where (merged_id not in (select merged_id from {staging_dataset}.{lang}_corpus)) "
                                  "and "
                                  f"(merged_id in (select merged_id from {production_dataset}.field_scores)))"),
                        "useLegacySql": False,
                        "destinationTable": {
                            "projectId": project_id,
                            "datasetId": staging_dataset,
                            "tableId": f"prev_{lang}_corpus"
                        },
                        "allowLargeResults": True,
                        "createDisposition": "CREATE_IF_NEEDED",
                        "writeDisposition": "WRITE_TRUNCATE"
                    }
                }
            )

        wait_for_backup >> copy_corpus >> success_alert
