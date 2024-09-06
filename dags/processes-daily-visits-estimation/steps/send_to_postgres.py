from __future__ import annotations

from airflow.hooks.base_hook import BaseHook
from airflow.models import DAG, TaskInstance, Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.hooks.compute_ssh import ComputeEngineSSHHook
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.utils.task_group import TaskGroup
from common.operators.bigquery import OlvinBigQueryOperator

def postgres_slack_alert(progress: str, db: str, db_instance: str, table: str):
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
            "msg_title": f"Daily ingestion into {db} db called {db_instance} Started",
            "notify_grp": "!here"
        },
        "completed": {
            "emoji": ":checkered_flag:", 
            "msg_title": f"Daily ingestion into {db} db called {db_instance} completed",
            "notify_grp": "!here"
        },
        "failed": {
            "emoji": ":skull:", 
            "msg_title": f"Daily ingestion for table {table} into {db} db called {db_instance} failed",
            "notify_grp": "@de_gcp"
        },
    }
    emoji = config_params.get(progress).get("emoji")
    msg_title = config_params.get(progress).get("msg_title")
    notify_grp = config_params.get(progress).get("notify_grp")

    # Getting the configuration for the progress that is being passed to the function.
    def submit_alert_config(*args, **kwargs):
        # Getting the slack token from the connection.
        slack_webhook_token = BaseHook.get_connection(
            "slack_notifications_postgres"
        ).password
        # Creating a message to be sent to Slack.
        slack_msg = f":almanac: <{notify_grp}> {msg_title} {emoji}"

        submit_alert = SlackWebhookHook(
            http_conn_id="slack_notifications_postgres",
            webhook_token=slack_webhook_token,
            message=slack_msg,
            username="airflow",
        )

        # Returning the response from the Slack API.
        submit_alert.execute()

    return submit_alert_config

def register(start: TaskInstance, dag: DAG) -> TaskGroup:
    if dag.params["stage"] == "staging":
        DB = "dev"
        POSTGRES_CONN_ID = dag.params["postgres-dev-conn-id"]
        DB_INSTANCE = "almanac-dev-f9129bae"
        BIGQUERY_PROJECT = Variable.get("env_project")
        PUBLISH = False
        MACHINE_NAME = "bq-staging-pg-dev-send"
    elif dag.params["stage"] == "production":
        DB = "staging"
        POSTGRES_CONN_ID = dag.params["postgres-staging-conn-id"]
        DB_INSTANCE = "almanac-staging"
        BIGQUERY_PROJECT = Variable.get("almanac_project")
        PUBLISH = True
        MACHINE_NAME = "bq-prod-pg-staging-send"

    POSTGRES_CONN = BaseHook.get_connection(POSTGRES_CONN_ID)
    HOSTNAME = POSTGRES_CONN.host
    PASSWORD = POSTGRES_CONN.password
    VM_ZONE = dag.params["pg-send-vm-zone"]
    final_tasks = []

    with TaskGroup(group_id="send-to-postgres") as group:

        upscale_db_vm = BashOperator(
            task_id="upscale_db_vm",
            bash_command=f"gcloud sql instances patch {DB_INSTANCE} "
            f"--project={Variable.get('postgres_project')} "
            "--cpu=64 "
            "--memory=256GB "
            "--database-flags max_connections=250,track_commit_timestamp=on,work_mem=1500000",
        )
        create_upload_vm = BashOperator(
            task_id="create_upload_vm",
            bash_command="{% include './include/bash/create.sh' %}",
            env={
                "INSTANCE": MACHINE_NAME,
                "PROJECT": Variable.get("env_project"),
                "ZONE": VM_ZONE,
                "MACHINE_TYPE": "n2-standard-64",
                "IMAGE_PROJECT": "centos-cloud",
                "SCOPES": "https://www.googleapis.com/auth/cloud-platform",
            },
        )
        start >> upscale_db_vm >> create_upload_vm

        def send_table(
        table: str, dataset_bigquery: str
    ):
        # Export to GCS
            delete_staging_pre_pg_transfer = GCSDeleteObjectsOperator(
                task_id=f"delete_staging_pre_pg_transfer_{table}",
                dag=dag,
                bucket_name=dag.params["postgres-staging-bucket"],
                # prefix=f"instance={DB}/{table}/local_date={{{{ ti.xcom_pull(task_ids='local-date') }}}}",
                prefix=f"instance={DB}/{table}",
            )

            create_upload_vm >> delete_staging_pre_pg_transfer

            export_staging = OlvinBigQueryOperator(
                task_id=f"export_staging_{table}",
                query="{% include './include/bigquery/send_to_postgres/export_staging.sql' %}",
                params={
                    "gcs-bucket": dag.params["postgres-staging-bucket"],
                    "db": DB,
                    "dataset_bigquery": dataset_bigquery,
                    "bigquery-project": BIGQUERY_PROJECT,
                    "table": table,
                },
            )
            delete_staging_pre_pg_transfer >> export_staging

            final_tasks.append(export_staging)
        # Delete possible matching data from database
       

        send_to_gcs_staging_end = EmptyOperator(task_id="send_to_gcs_staging_end",
                                                on_success_callback=postgres_slack_alert(
                    progress="start",
                    db=DB,
                    db_instance=DB_INSTANCE,
                    table=""
                ),)
        send_to_postgres_end = EmptyOperator(task_id="send_to_postgres_end",
                                             on_success_callback=postgres_slack_alert(
                    progress="completed",
                    db=DB,
                    db_instance=DB_INSTANCE,
                    table=""
                ),)
        # Setting the previous task to the send_to_staging_end task.
        prev_task = send_to_gcs_staging_end
        # Setting the previous task to the create_upload_vm task.
        for table in dag.params["postgres_db_raw_tables"]:
            dataset_bigquery="postgres_rt"
            send_table(
                    table=table,
                    dataset_bigquery=dataset_bigquery,
                )
            delete_operator = PostgresOperator(
                task_id=f"delete_{table}",
                postgres_conn_id=POSTGRES_CONN_ID,
                sql=f"TRUNCATE TABLE {table.lower()} ",
                # sql=f"DELETE FROM `{table.lower()}` WHERE local_date >= {{ ti.xcom_pull(task_ids='local-date') }} ",
            )
            prev_task >> delete_operator

            copy_job = SSHOperator(
                task_id=f"copy_{table}",
                ssh_hook=ComputeEngineSSHHook(
                    instance_name=MACHINE_NAME,
                    zone=VM_ZONE,
                    project_id=Variable.get("env_project"),
                    use_oslogin=True,
                    use_iap_tunnel=False,
                    use_internal_ip=True,
                ),
                command="{% include './include/bash/copy.sh' %}",
                cmd_timeout=None,
                retries=1,
                retry_delay=60 * 3,
                on_failure_callback=postgres_slack_alert(
                    progress="failed",
                    db=DB,
                    db_instance=DB_INSTANCE,
                    table=f"{dataset_bigquery}.{table}"
                ),
                params={
                    "DIRNAME": table,
                    "TABLE": table,
                    "FILENAME": "combined",
                    "FILE_SUFFIX": "part",
                    "GCS_PREFIX": f"instance={DB}/{table}",  # local_date part is in script.
                    "HOSTNAME": HOSTNAME,
                    "PASSWORD": PASSWORD,
                },
            )
            delete_operator >> copy_job

            # Delete staging data
            delete_gcs_staging_post_pg_transfer = GCSDeleteObjectsOperator(
                task_id=f"delete_gcs_staging_post_pg_transfer_{table}",
                bucket_name=dag.params["postgres-staging-bucket"],
                prefix="instance={{ params['db'] }}/{{ params['table'] }}",
                params={"db": DB, "table": table},
            )
            copy_job >> delete_gcs_staging_post_pg_transfer
            prev_task = delete_gcs_staging_post_pg_transfer

        for table in dag.params["postgres_db_mv_tables"]:
            dataset_bigquery="postgres_mv_rt"
            send_table(
                    table=table,
                    dataset_bigquery=dataset_bigquery,
                )

            delete_operator = PostgresOperator(
                task_id=f"delete_{table}",
                postgres_conn_id=POSTGRES_CONN_ID,
                sql=f"TRUNCATE TABLE {table.lower()} ",
                # sql=f"DELETE FROM `{table.lower()}` WHERE local_date >= {{ ti.xcom_pull(task_ids='local-date') }} ",
            )
            prev_task >> delete_operator

            copy_job = SSHOperator(
                task_id=f"copy_{table}",
                ssh_hook=ComputeEngineSSHHook(
                    instance_name=MACHINE_NAME,
                    zone=VM_ZONE,
                    project_id=Variable.get("env_project"),
                    use_oslogin=True,
                    use_iap_tunnel=False,
                    use_internal_ip=True,
                ),
                retries=1,
                retry_delay=60 * 3,
                on_failure_callback=postgres_slack_alert(
                    progress="failed",
                    db=DB,
                    db_instance=DB_INSTANCE,
                    table=f"{dataset_bigquery}.{table}"
                ),
                command="{% include './include/bash/copy.sh' %}",
                cmd_timeout=None,
                params={
                    "DIRNAME": table,
                    "TABLE": table,
                    "FILENAME": "combined",
                    "FILE_SUFFIX": "part",
                    "GCS_PREFIX": f"instance={DB}/{table}",  # local_date part is in script.
                    "HOSTNAME": HOSTNAME,
                    "PASSWORD": PASSWORD,
                },
            )
            delete_operator >> copy_job

            # Delete staging data
            delete_gcs_staging_post_pg_transfer = GCSDeleteObjectsOperator(
                task_id=f"delete_gcs_staging_post_pg_transfer_{table}",
                bucket_name=dag.params["postgres-staging-bucket"],
                prefix="instance={{ params['db'] }}/{{ params['table'] }}",
                params={"db": DB, "table": table},
            )
            copy_job >> delete_gcs_staging_post_pg_transfer
            prev_task = delete_gcs_staging_post_pg_transfer

        final_tasks >> send_to_gcs_staging_end

        downscale_db_vm = BashOperator(
            task_id="downscale_db_vm",
            bash_command=(
                f"gcloud sql instances patch {DB_INSTANCE} "
                f"--project={Variable.get('postgres_project')} "
                "--cpu=4 "
                "--memory=13GB "
                "--database-flags max_connections=250,track_commit_timestamp=on,work_mem=32768"
            ),
        )
        delete_gcs_staging_post_pg_transfer >> downscale_db_vm

        delete_upload_vm = BashOperator(
            task_id="delete_upload_vm",
            bash_command=(
                f"gcloud compute instances delete {MACHINE_NAME} "
                f"--project {Variable.get('env_project')} "
                f"--zone {VM_ZONE}"
            ),
        )
        downscale_db_vm >> delete_upload_vm >> send_to_postgres_end

        if PUBLISH:
            delay_to_wait_for_index_refresh = BashOperator(
                task_id="delay_to_wait_for_index_refresh",
                bash_command="sleep 3600"
            )

            publish = BashOperator(
                task_id="publish",
                bash_command="{% include './include/bash/publish.sh' %}",
                env={
                    "TOPIC": "staging-to-prod",
                    "PROJECT": Variable.get("almanac_project"),
                },
            )
            send_to_postgres_end >> delay_to_wait_for_index_refresh >> publish

    with TaskGroup(group_id="send-to-gcs") as gcs_send_group:
        send_tables_end = EmptyOperator(task_id=f"exportToGcs_end")

        def send_tables(table: str, dataset_bigquery: str, start=send_to_postgres_end):
            delete_gcs_daily = GCSDeleteObjectsOperator(
                        task_id=f"delete_daily_{table}",
                        dag=dag,
                        bucket_name=dag.params["postgres-daily-bucket"],
                        prefix=f"instance={DB}/{table}/local_date={{{{ ti.xcom_pull(task_ids='local-date') }}}}",
                    )

            export_daily = OlvinBigQueryOperator(
                task_id=f"export_daily_{table}",
                query="{% include './include/bigquery/send_to_postgres/export_daily.sql' %}",
                params={
                    "gcs-bucket": dag.params["postgres-daily-bucket"],
                    "db": DB,
                    "dataset_bigquery": dataset_bigquery,
                    "bigquery-project": BIGQUERY_PROJECT,
                    "table": table,
                },
            )
            start >> delete_gcs_daily >> export_daily >> send_tables_end

        # send tables
        for table in dag.params["postgres_db_tables_in_bq"]:
            send_tables(table=f"{table}", dataset_bigquery="postgres_rt")

    start >> group >> gcs_send_group



    return gcs_send_group
