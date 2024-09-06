"""
DAG ID: demographics_pipeline
"""
import csv
import logging
from tempfile import NamedTemporaryFile

import pandas as pd
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.google.cloud.hooks.compute_ssh import ComputeEngineSSHHook
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from airflow.operators.empty import EmptyOperator

def postgres_slack_alert(progress: str, db: str, db_instance: str):
    """
    The above function is used to send a slack notification to the user

    Args:
      progress (str): This is the progress of the job. It can be start, end, failed, or completed.
      job (str): The name of the job.
      table (str): The name of the table that is being processed.

    Returns:
      The return value is the response from the Slack API.
    """
    config_params = {
        "start": {
            "emoji": ":hourglass_flowing_sand:",
            "msg_title": f"On demand ingestion into {db} db called {db_instance} Started",
        },
        "completed": {"emoji": ":checkered_flag:", "msg_title": f"On demand ingestion into {db} db called {db_instance} completed"},
    }
    emoji = config_params.get(progress).get("emoji")
    msg_title = config_params.get(progress).get("msg_title")

    # Getting the configuration for the progress that is being passed to the function.
    def submit_alert_config(*args, **kwargs):
        # Getting the slack token from the connection.
        slack_webhook_token = BaseHook.get_connection(
            "slack_notifications_postgres"
        ).password
        # Creating a message to be sent to Slack.
        slack_msg = f":almanac: <!here> {msg_title} {emoji}"

        submit_alert = SlackWebhookHook(
            http_conn_id="slack_notifications_postgres",
            webhook_token=slack_webhook_token,
            message=slack_msg,
            username="airflow",
        )

        # Returning the response from the Slack API.
        submit_alert.execute()

    return submit_alert_config

def register(dag, start, db):
    if db == "dev":
        POSTGRES_CONN_ID = dag.params["postgres_dev_conn_id"]
        DB_INSTANCE = "almanac-dev-f9129bae"
        POSTGRES_CONN = BaseHook.get_connection(POSTGRES_CONN_ID)
        HOSTNAME = POSTGRES_CONN.host
        PASSWORD = POSTGRES_CONN.password
    elif db == "staging":
        POSTGRES_CONN_ID = dag.params["postgres_staging_conn_id"]
        DB_INSTANCE = "almanac-staging"
        POSTGRES_CONN = BaseHook.get_connection(POSTGRES_CONN_ID)
        HOSTNAME = POSTGRES_CONN.host
        PASSWORD = POSTGRES_CONN.password

    MACHINE_NAME="pg-db-send-append-batch"
    VM_ZONE = dag.params["vm_zone"]
    final_tasks = []

    upscale_db_vm = BashOperator(
        task_id="upscale_db_vm",
        bash_command=f"gcloud sql instances patch {DB_INSTANCE} "
    f"--project={Variable.get('postgres_project')} "
    "--cpu=64 "
    "--memory=256GB "
    "--database-flags max_connections=250,track_commit_timestamp=on,work_mem=1500000",
        dag=dag,
    )

    create_upload_vm = BashOperator(
        task_id="create_upload_vm",
        bash_command="{% include './include/bash/create.sh' %}",
        env={
            "INSTANCE": MACHINE_NAME,
            "PROJECT": Variable.get('env_project'),
            "ZONE": VM_ZONE,
            "MACHINE_TYPE": "n2-standard-64",
            "IMAGE_PROJECT": "centos-cloud",
            "SCOPES": "https://www.googleapis.com/auth/cloud-platform",
        },
        dag=dag,
    )

    start_transfer = EmptyOperator(task_id="start_transfer",
                                                on_success_callback=postgres_slack_alert(
                    progress="start",
                    db=db,
                    db_instance=DB_INSTANCE,
                ),)
    end_transfer = EmptyOperator(task_id="end_transfer",
                                             on_success_callback=postgres_slack_alert(
                    progress="completed",
                    db=db,
                    db_instance=DB_INSTANCE,
                ),)

    start >> upscale_db_vm >> create_upload_vm >> start_transfer
    def send_tables(dataset_bigquery: str, table_bigquery: str, database_table: str):
        # Export to GCS
        delete_gcs_staging_pre = GCSDeleteObjectsOperator(
                task_id=f"delete_gcs_staging_pre_{database_table}",
                depends_on_past=False,
                dag=dag,
                bucket_name=dag.params["staging_bucket"],
                prefix=f"{database_table}/",
            )
         
        export_job = BigQueryInsertJobOperator(
            task_id=f"export_to_GCS_{database_table}",
            project_id=dag.params["project"],
            configuration={
                "extract": {
                    "destinationUris": (
                        (
                            f"gs://{{{{ params['staging_bucket'] }}}}/{database_table}/*.csv"
                        )
                    ),
                    "printHeader": True,
                    "destinationFormat": "CSV",
                    "fieldDelimiter": "\t",
                    # "compression": "GZIP",
                    "sourceTable": {
                        "projectId": f"{{{{ params['project'] }}}}",
                        "datasetId": dataset_bigquery,
                        "tableId": table_bigquery,
                    },
                }
            },
            dag=dag,
        )
        start_transfer >> delete_gcs_staging_pre >> export_job

        final_tasks.append(export_job)

    send_to_staging_end = DummyOperator(task_id="send_to_staging_end")
    prev_task = send_to_staging_end
    for table in dag.params["tables_list"]:
        send_tables(
                dataset_bigquery=dag.params["tables_dataset"],
                table_bigquery=f"{table}",
                database_table=f"{table}",
            )
        delete_operator = PostgresOperator(
            task_id=f"delete_pg_{table}",
            postgres_conn_id=POSTGRES_CONN_ID,
            sql="DELETE FROM {{ params['table'] }} WHERE local_date >= {{ params['local_date'] }} ",
            params={
                "table": table.lower(),
                "local_date": f"{{{{ execution_date.subtract(days={Variable.get('latency_daily_feed')}).strftime('%Y-%m-%d') }}}}",
            },
        )

        copy_job = SSHOperator(
            task_id=f"copy_pg_{table}",
            ssh_hook=ComputeEngineSSHHook(
                instance_name=MACHINE_NAME,
                zone=f"{dag.params['vm_zone']}",
                project_id=f"{Variable.get('env_project')}",
                use_oslogin=True,
                use_iap_tunnel=False,
                use_internal_ip=True,
            ),
            command=f"{{% include './include/bash/copy.sh' %}}",
            params={
                "DIRNAME": f"{table}",
                "TABLE": f"{table}",
                "FILENAME": "combined",
                "FILE_SUFFIX": "part",
                "GCS_PREFIX": f"{table}",
                "HOSTNAME": HOSTNAME,
                "PASSWORD": PASSWORD,
            },
            cmd_timeout=None,
            retries=1,
            retry_delay=60 * 3,
        )

        delete_gcs_staging_post = GCSDeleteObjectsOperator(
            task_id=f"delete_gcs_staging_post_{table}",
            depends_on_past=False,
            dag=dag,
            bucket_name=dag.params["staging_bucket"],
            prefix=f"{table}/",
        )

        (
            prev_task
            >> delete_operator
            >> copy_job
            >> delete_gcs_staging_post
        )
        prev_task = delete_gcs_staging_post


    final_tasks >> send_to_staging_end

    downscale_db_vm = BashOperator(
        task_id="downscale_db_vm",
        bash_command=f"gcloud sql instances patch {DB_INSTANCE} "
    f"--project={Variable.get('postgres_project')} "
    "--cpu=4 "
    "--memory=13GB "
    "--database-flags max_connections=250,track_commit_timestamp=on,work_mem=32768",
        dag=dag,
    )
    delete_gcs_staging_post >> end_transfer >> downscale_db_vm

    delete_upload_vm = BashOperator(
        task_id="delete_upload_vm",
        bash_command=f"gcloud compute instances delete {MACHINE_NAME} "
        f"--project {Variable.get('env_project')} "
        f"--zone {VM_ZONE}",
        dag=dag,
    )
    downscale_db_vm >> delete_upload_vm
    return delete_upload_vm
