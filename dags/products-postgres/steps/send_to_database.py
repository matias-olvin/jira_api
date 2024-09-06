"""
DAG ID: demographics_pipeline
"""
from airflow.models import DAG, TaskInstance, Variable
from airflow.utils.task_group import TaskGroup
from airflow.hooks.base_hook import BaseHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.hooks.compute_ssh import ComputeEngineSSHHook
from airflow.providers.ssh.operators.ssh import SSHOperator


def postgres_slack_alert(progress: str, job: str, table=""):
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
            "msg_title": f"{table} - {job} Started",
        },
        "end": {"emoji": ":checkered_flag:", "msg_title": f"{table} - {job} Completed"},
        "failed": {"emoji": ":skull:", "msg_title": f"{table} - {job} Failed"},
        "completed": {"emoji": ":partying_face:", "msg_title": f"{job} Completed!"},
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


def register(dag, start, db) -> TaskGroup:
    """
    It takes a list of tables and views, and then it uploads the data from BigQuery to Postgres, and
    then it refreshes the views

    Args:
      dag: The DAG object.
      start: The start of the DAG.

    Returns:
      The refresh_views_end task.
    """
    if db == "dev":
        POSTGRES_CONN_ID = dag.params["postgres_dev_conn_id"]
        DB_INSTANCE = "almanac-dev-f9129bae"
        POSTGRES_CONN = BaseHook.get_connection(POSTGRES_CONN_ID)
        HOSTNAME = POSTGRES_CONN.host
        PASSWORD = POSTGRES_CONN.password
    elif db == "staging":
        POSTGRES_CONN_ID = dag.params["postgres_staging_conn_id"]
        DB_INSTANCE = ""
        POSTGRES_CONN = BaseHook.get_connection(POSTGRES_CONN_ID)
        HOSTNAME = POSTGRES_CONN.host
        PASSWORD = POSTGRES_CONN.password
    MACHINE_NAME = "postgres-db-send"
    VM_ZONE = dag.params["pg_send_vm_zone"]
    final_tasks = []
    with TaskGroup(group_id="send-to-postgres") as group:
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
            bash_command="{% include './bash/create.sh' %}",
            env={
                "INSTANCE": MACHINE_NAME,
                "PROJECT": Variable.get("env_project"),
                "ZONE": VM_ZONE,
                "MACHINE_TYPE": "n2-standard-64",
                "IMAGE_PROJECT": "centos-cloud",
                "SCOPES": "https://www.googleapis.com/auth/cloud-platform",
            },
            dag=dag,
        )
        start >> upscale_db_vm >> create_upload_vm

        def send_tables(
            dataset_bigquery: str, table_bigquery: str, database_table: str
        ):
            # Export to GCS
            delete_gcs_staging_pre = GCSDeleteObjectsOperator(
                task_id=f"delete_gcs_staging_pre_{database_table}",
                depends_on_past=False,
                dag=dag,
                bucket_name=dag.params["postgres_staging_bucket"],
                prefix=f"{database_table}/",
            )

            export_gcs_staging = BigQueryInsertJobOperator(
                task_id=f"export_gcs_staging_{database_table}",
                project_id=f"{Variable.get('env_project')}",
                configuration={
                    "extract": {
                        "destinationUris": (
                            f"gs://{{{{ params['postgres_staging_bucket'] }}}}/{database_table}/*.csv"
                        ),
                        "printHeader": True,
                        "destinationFormat": "CSV",
                        "fieldDelimiter": "\t",
                        # "compression": "GZIP",
                        "sourceTable": {
                            "projectId": f"{Variable.get('env_project')}",
                            "datasetId": dataset_bigquery,
                            "tableId": table_bigquery,
                        },
                    }
                },
                dag=dag,
            )

            (
                create_upload_vm
                >> delete_gcs_staging_pre
                >> export_gcs_staging
            )
            final_tasks.append(export_gcs_staging)
    # A variable that is used to send a slack notification to the user.
        job = "Upload"
        # A dummy operator that is used to send a slack notification to the user.
        send_to_staging_end = DummyOperator(task_id="send_to_staging_end")

        # Setting the previous task to the send_to_staging_end task.
        prev_task = send_to_staging_end
        # Creating a task for each table in the list of tables to be uploaded to Postgres.
        for table in dag.params["postgres_db_raw_tables"]:
            # Calling the function send_tables with the parameters table_bigquery and database_table.
            send_tables(
                    dataset_bigquery="postgres_batch",
                    table_bigquery=f"{table}",
                    database_table=f"{table}",
                )
            # Sending a slack notification to the user.
            upload_alert = DummyOperator(
                task_id=f"alert_{table}",
                dag=dag,
                on_success_callback=postgres_slack_alert(
                    progress="start",
                    job=f"{job}",
                    table=f"{table}",
                ),
            )
            # Delete possible matching data from database
            delete_operator = PostgresOperator(
                task_id=f"delete_{table}",
                dag=dag,
                postgres_conn_id=POSTGRES_CONN_ID,
                sql=f"TRUNCATE TABLE {table.lower()} ",
                # sql=f"DELETE FROM {table.lower()} WHERE local_date>={{{{ execution_date.subtract(days=4).strftime('%Y-%m-%d') }}}} ",
            )
            # Uploading the data from GCS to Postgres.
            copy_job = SSHOperator(
                    task_id=f"copy_job_{table}",
                    ssh_hook=ComputeEngineSSHHook(
                        instance_name=MACHINE_NAME,
                        zone=f"{dag.params['pg_send_vm_zone']}",
                        project_id=f"{Variable.get('env_project')}",
                        use_oslogin=True,
                        use_iap_tunnel=False,
                        use_internal_ip=True,
                    ),
                    command="{% include './bash/copy.sh' %}",
                    params={
                        "DIRNAME": f"{table}",
                        "TABLE": f"{table}",
                        "FILENAME": "combined",
                        "FILE_SUFFIX": "part",
                        "GCS_PREFIX": f"{table}",
                        "HOSTNAME": f"{HOSTNAME}",
                        "PASSWORD": f"{PASSWORD}",
                    },
                    cmd_timeout=None,
                    retries=1,
                    retry_delay=60 * 3,
                    on_failure_callback=postgres_slack_alert(
                    progress="failed", job=f"{job}", table=f"{table}"
                ),
                on_success_callback=postgres_slack_alert(
                    progress="end", job=f"{job}", table=f"{table}"
                ),
                )
            # Delete staging data
            delete_gcs_staging_post = GCSDeleteObjectsOperator(
                task_id=f"delete_gcs_staging_post_{table}",
                depends_on_past=False,
                dag=dag,
                bucket_name=dag.params["postgres_staging_bucket"],
                prefix=f"{table}/",
            )
            (
                prev_task
                >> upload_alert
                >> delete_operator
                >> copy_job
                >> delete_gcs_staging_post
            )
            prev_task = delete_gcs_staging_post

        for table in dag.params["postgres_db_mv_tables"]:
            # Calling the function send_tables with the parameters table_bigquery and database_table.
            send_tables(
                    dataset_bigquery="postgres_mv_batch",
                    table_bigquery=f"{table}",
                    database_table=f"{table}",
                )
            # Sending a slack notification to the user.
            upload_alert = DummyOperator(
                task_id=f"alert_{table}",
                dag=dag,
                on_success_callback=postgres_slack_alert(
                    progress="start",
                    job=f"{job}",
                    table=f"{table}",
                ),
            )
            # Delete possible matching data from database
            delete_operator = PostgresOperator(
                task_id=f"delete_{table}",
                dag=dag,
                postgres_conn_id=POSTGRES_CONN_ID,
                sql=f"TRUNCATE TABLE {table.lower()} ",
                # sql=f"DELETE FROM {table.lower()} WHERE local_date>={{{{ execution_date.subtract(days=4).strftime('%Y-%m-%d') }}}} ",
            )
            # Uploading the data from GCS to Postgres.
            copy_job = SSHOperator(
                    task_id=f"copy_job_{table}",
                    ssh_hook=ComputeEngineSSHHook(
                        instance_name=MACHINE_NAME,
                        zone=f"{dag.params['pg_send_vm_zone']}",
                        project_id=f"{Variable.get('env_project')}",
                        use_oslogin=True,
                        use_iap_tunnel=False,
                        use_internal_ip=True,
                    ),
                    command="{% include './bash/copy.sh' %}",
                    params={
                        "DIRNAME": f"{table}",
                        "TABLE": f"{table}",
                        "FILENAME": "combined",
                        "FILE_SUFFIX": "part",
                        "GCS_PREFIX": f"{table}",
                        "HOSTNAME": f"{HOSTNAME}",
                        "PASSWORD": f"{PASSWORD}",
                    },
                    cmd_timeout=None,
                    retries=1,
                    retry_delay=60 * 3,
                    on_failure_callback=postgres_slack_alert(
                    progress="failed", job=f"{job}", table=f"{table}"
                ),
                on_success_callback=postgres_slack_alert(
                    progress="end", job=f"{job}", table=f"{table}"
                ),
                )
            # Delete staging data
            delete_gcs_staging_post = GCSDeleteObjectsOperator(
                task_id=f"delete_gcs_staging_post_{table}",
                depends_on_past=False,
                dag=dag,
                bucket_name=dag.params["postgres_staging_bucket"],
                prefix=f"{table}/",
            )
            (
                prev_task
                >> upload_alert
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

        delete_gcs_staging_post >> downscale_db_vm

        

        delete_upload_vm = BashOperator(
            task_id="delete_upload_vm",
            bash_command=f"gcloud compute instances delete {MACHINE_NAME} "
            f"--project {Variable.get('env_project')} "
            f"--zone {VM_ZONE}",
            dag=dag,
        )
        # A dummy operator that is used to send a slack notification to the user.
        send_to_database_end = DummyOperator(
            task_id="send_to_database_end",
            # Setting the previous task to the send_to_staging_end task.
            on_success_callback=postgres_slack_alert(
                progress="completed",
                job=f"{job}",
            ),
        )
        downscale_db_vm >> delete_upload_vm >> send_to_database_end

        start >> group

    # with TaskGroup(group_id="refresh-views") as refresh_views_group:
    #     # A variable that is used to send a slack notification to the user.
    #     job = "Refresh View"
    #     # Creating a dummy operator that will be used to send a slack notification to the user.
    #     refresh_views_end = DummyOperator(
    #         task_id="refreshed_views_end",
    #         on_success_callback=postgres_slack_alert(
    #             progress="completed",
    #             job=f"{job}",
    #         ),
    #     )
    #     # Setting the previous task to the send_to_database_end task.
    #     prev_task = send_to_database_end
    #     # Creating a task for each view in the list of views to be refreshed.
    #     for materialized_view in dag.params["refresh_view_tables"]:
    #         # Sending a slack notification to the user.
    #         refresh_views_alert = DummyOperator(
    #             task_id=f"alert_{materialized_view}",
    #             dag=dag,
    #             on_success_callback=postgres_slack_alert(
    #                 progress="start",
    #                 job=f"{job}",
    #                 table=f"{materialized_view}",
    #             ),
    #         )
    #         # Refreshing the materialized view.
    #         refresh_operator = PostgresOperator(
    #             task_id=f"refresh_view_{materialized_view}",
    #             dag=dag,
    #             postgres_conn_id=dag.params["postgres_conn_id"],
    #             sql=f"CALL private.refresh_materialized_view('{materialized_view}'); ",
    #             on_failure_callback=postgres_slack_alert(
    #                 progress="failed", job=f"{job}", table=f"{materialized_view}"
    #             ),
    #             on_success_callback=postgres_slack_alert(
    #                 progress="end", job=f"{job}", table=f"{materialized_view}"
    #             ),
    #         )
    #         prev_task >> refresh_views_alert >> refresh_operator
    #         prev_task = refresh_operator
    #     refresh_operator >> refresh_views_end

    return group