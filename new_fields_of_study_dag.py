"""
Updates the fields of study predictions. To only run on new records or records with title/abstract text that has
changed since the last run, trigger this dag with no parameters. To force the dag to rerun on everything,
trigger the dag with the configuration {"rerun": true}
"""

import json
import os
import re

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator, BigQueryCheckOperator
from airflow.providers.google.cloud.transfers.bigquery_to_bigquery import BigQueryToBigQueryOperator
from airflow.providers.google.cloud.operators.compute import (
    ComputeEngineDeleteInstanceOperator,
    ComputeEngineInsertInstanceOperator,
    ComputeEngineStartInstanceOperator,
    ComputeEngineStopInstanceOperator
)
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook
from datetime import datetime

from dataloader.airflow_utils.defaults import DATA_BUCKET, PROJECT_ID, GCP_ZONE, DAGS_DIR, get_default_args, \
    get_post_success
from dataloader.scripts.populate_documentation import update_table_descriptions
from dataloader.scripts.clean_backups import clean_backups


GCP_REGION = re.sub(r'-[a-f]$', '', GCP_ZONE)

production_dataset = "fields_of_study_v2"
staging_dataset = f"staging_{production_dataset}"

pipeline_args = get_default_args(pocs=["Rebecca"])
pipeline_args["retries"] = 1

def mk_command_seq(cmds: list) -> str:
    scripts = " && ".join(cmds)
    return (f"gcloud compute ssh airflow@{gce_resource_id} --zone {GCP_ZONE} "
                f"--command \"{scripts}\"")


with DAG("new_fields_of_study",
            default_args=pipeline_args,
            description="Labels our scholarly literature with fields of study",
            schedule_interval=None,
            user_defined_macros = {"staging_dataset": staging_dataset, "production_dataset": production_dataset},
            catchup=False
         ) as dag:
    slack_webhook = BaseHook.get_connection("slack")
    bucket = DATA_BUCKET
    tmp_dir = f"{production_dataset}/tmp"
    outputs_dir = f"{tmp_dir}/outputs"
    schema_dir = f"{production_dataset}/schemas"
    sql_dir = f"sql/{production_dataset}"
    backups_dataset = f"{production_dataset}_backups"
    gce_resource_id = "fos-runner"
    bq_labels = {"dataset": "fields_of_study_v2"}
    service_account = "fields-of-study@gcp-cset-projects.iam.gserviceaccount.com"
    disk_image = "projects/ml-images/global/images/c0-deeplearning-common-cpu-v20250310-debian-11"  # noqa

    # We keep script inputs and outputs in a tmp dir on gcs, so clean it out at the start of each run. We clean at
    # the start of the run so if the run fails we can examine the failed data
    clear_tmp_dir = GCSDeleteObjectsOperator(
        task_id="clear_tmp_gcs_dir",
        bucket_name=bucket,
        prefix=tmp_dir + "/"
    )

    create_instance = ComputeEngineInsertInstanceOperator(
        task_id="create-"+gce_resource_id,
        project_id=PROJECT_ID,
        zone=GCP_ZONE,
        # See https://cloud.google.com/compute/docs/reference/rest/v1/instances/insert
        body={
            "name": gce_resource_id,
            "machine_type": f"zones/{GCP_ZONE}/machineTypes/n2-standard-8",
            "service_accounts": [{
                "email": service_account,
                "scopes": [
                    "https://www.googleapis.com/auth/bigquery",
                    "https://www.googleapis.com/auth/devstorage.read_write",
                    "https://www.googleapis.com/auth/logging.write",
                    "https://www.googleapis.com/auth/monitoring.write",
                    "https://www.googleapis.com/auth/service.management.readonly",
                    "https://www.googleapis.com/auth/servicecontrol",
                    "https://www.googleapis.com/auth/trace.append",
                ],
            }],
            "disks": [{
                "boot": True,
                "mode": "rw",

                "initialize_params": {
                    "disk_name": "fos-runner",
                    "source_image": disk_image,
                    "disk_type": f"zones/{GCP_ZONE}/diskTypes/pd-standard",
                    "disk_size_gb": 1_000,
                },
                "auto_delete": True,
                "disk_size_gb": 1_000,

            }],
            "network_interfaces": [{
                "stack_type": "IPV4_ONLY",
                "access_configs": [{
                    "network_tier": "STANDARD",
                }],
                "subnetwork": f"regions/{GCP_REGION}/subnetworks/default",
            }]

        },
    )

    # start the instance where we'll run the download and scoring scripts
    gce_instance_start = ComputeEngineStartInstanceOperator(
        project_id=PROJECT_ID,
        zone=GCP_ZONE,
        resource_id=gce_resource_id,
        task_id="start-"+gce_resource_id
    )

    # clear out the directory of code and dvc artifacts on each run and grab whatever's
    # latest on GCS (to be updated by the push_to_airflow script)
    refresh_artifacts = BashOperator(
        task_id=f"refresh_artifacts",
        bash_command=mk_command_seq([
            f"gsutil -m cp -r gs://{bucket}/{production_dataset}/fields-of-study-pipeline/scripts/setup-vm.sh .",
            "bash setup-vm.sh",
        ])
    )

    clear_tmp_dir >> create_instance >> gce_instance_start >> refresh_artifacts

    prev_op = refresh_artifacts

    languages = ["en"]
    for lang in languages:
        # run the download script; filter inputs to only "changed" rows if the user did not pass the "rerun" param
        # through the dagrun config
        download = BashOperator(
            task_id=f"download_{lang}",
            bash_command = mk_command_seq([
                # make sure the corpus dir is clean
                f"rm -rf assets/corpus/* || true",
                "source ~/miniconda3/bin/activate",
                (f"PYTHONPATH=. conda run -n fos python scripts/download_corpus.py"
                 f" {lang} "
                 "{{'' if dag_run and dag_run.conf.get('rerun') else '--skip_prev'}} "
                    f"--use_default_clients --bq_dest {staging_dataset} --extract_bucket {bucket} "
                    f"--extract_prefix {tmp_dir}/inputs/{lang}_corpus-")
            ])
        )

        score_corpus = BashOperator(
            task_id=f"score_corpus_{lang}",
            bash_command=mk_command_seq([
                "source ~/miniconda3/bin/activate",
                f"PYTHONPATH=. conda run -n fos python "
                f"scripts/batch_score_corpus_constrained.py --limit 0",
                f"gsutil cp assets/corpus/{lang}_scores.jsonl gs://{bucket}/{outputs_dir}/"
            ])
        )

        load = GCSToBigQueryOperator(
            task_id=f"import_{lang}",
            bucket=bucket,
            source_objects=[f"{outputs_dir}/{lang}_scores.jsonl"],
            destination_project_dataset_table=f"{staging_dataset}.new_{lang}",
            source_format="NEWLINE_DELIMITED_JSON",
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_TRUNCATE",
            autodetect=True,
            labels=bq_labels,
        )

        prev_op >> download >> score_corpus >> load
        prev_op = load

    # stop the instance
    gce_instance_stop = ComputeEngineStopInstanceOperator(
        project_id=PROJECT_ID,
        zone=GCP_ZONE,
        resource_id=gce_resource_id,
        task_id="stop-"+gce_resource_id
    )

    prev_op >> gce_instance_stop
    prev_op = gce_instance_stop

    # Run the downstream queries in the order they appear in query_sequence.txt
    with open(f"{DAGS_DIR}/sequences/{production_dataset}/query_sequence.txt") as f:
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
                            "projectId": PROJECT_ID,
                            "datasetId": staging_dataset,
                            "tableId": table_name
                        },
                        "allowLargeResults": True,
                        "createDisposition": "CREATE_IF_NEEDED",
                        "writeDisposition": "WRITE_TRUNCATE",
                        "labels": bq_labels,
                    }
                }
            )
            prev_op >> query
            prev_op = query

    # Run this query separately because it's a DDL query specifying clustering fields
    query = BigQueryInsertJobOperator(
        task_id=f"run_top_fields",
        configuration={
            "query": {
                "query": "{% include '" + f"{sql_dir}/top_fields.sql" + "' %}",
                "useLegacySql": False,
                "labels": bq_labels,
            }
        }
    )
    prev_op >> query
    prev_op = query

    wait_for_checks = DummyOperator(task_id="wait_for_checks")

    for query in os.listdir(f"{DAGS_DIR}/{sql_dir}"):
        if not query.startswith("check_"):
            continue
        check = BigQueryCheckOperator(
            task_id=query.replace(".sql", ""),
            sql=f"{sql_dir}/{query}",
            params={
                "dataset": staging_dataset
            },
            use_legacy_sql=False,
            labels=bq_labels,
        )
        prev_op >> check >> wait_for_checks

    delete_instance = ComputeEngineDeleteInstanceOperator(
        task_id=f"delete-{gce_resource_id}",
        resource_id=gce_resource_id,
        zone=GCP_ZONE,
    )
    wait_for_checks >> delete_instance

    wait_for_backup = DummyOperator(task_id="wait_for_backup")

    curr_date = datetime.now().strftime('%Y%m%d')
    # copy to production, populate table descriptions, backup tables
    with open(f"{DAGS_DIR}/schemas/{production_dataset}/tables.json") as f:
        table_desc = json.loads(f.read())
    for table in ["field_scores", "top_fields", "field_meta", "field_hierarchy",
                  "field_children"]:
        prod_table_name = f"{production_dataset}.{table}"
        table_copy = BigQueryToBigQueryOperator(
            task_id=f"copy_{table}_to_production",
            source_project_dataset_tables=[f"{staging_dataset}.{table}"],
            destination_project_dataset_table=prod_table_name,
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_TRUNCATE",
            labels=bq_labels,
        )
        pop_descriptions = PythonOperator(
            task_id="populate_column_documentation_for_" + table,
            op_kwargs={
                "input_schema": f"{DAGS_DIR}/schemas/{production_dataset}/{table}.json",
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
            write_disposition="WRITE_TRUNCATE",
            labels=bq_labels,
        )

        delete_instance >> table_copy >> pop_descriptions >> table_backup >> wait_for_backup

    update_archive = PythonOperator(
        task_id="update_archive",
        op_kwargs={"dataset": backups_dataset, "backup_prefix": production_dataset},
        python_callable=clean_backups,
    )
    success_alert = get_post_success("Fields of study v2 update succeeded!", dag)
    update_archive >> success_alert

    # as a final step before posting success, update the prev_{lang}_corpus tables so we'll know what text we used
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
                            "projectId": PROJECT_ID,
                            "datasetId": staging_dataset,
                            "tableId": f"prev_{lang}_corpus"
                        },
                        "allowLargeResults": True,
                        "createDisposition": "CREATE_IF_NEEDED",
                        "writeDisposition": "WRITE_TRUNCATE",
                        "labels": bq_labels,
                    }
                }
            )

        wait_for_backup >> copy_corpus >> update_archive
