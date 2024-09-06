from __future__ import annotations

from typing import Union

from airflow.models import DAG, TaskInstance
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup


def register(start: Union[TaskGroup, TaskInstance], dag: DAG) -> TaskGroup:
    """
    Register tasks on the dag

    Args:
        start (airflow.models.TaskInstance): Task instance all tasks will
        be registered downstream from.
        dag (airflow.models.DAG): The dag to register the tasks on.

    Returns:
        airflow.models.TaskInstance: The task instance that the next task
        will be registered downstream from.
    """
    with TaskGroup("pby_azure_push") as pby_azure_push_group:

        # vision works
        account_name = "edpprodscusdl2"
        container_name = "pass-by-incoming"
        client_token_txt_file_name = "visionworks.txt"
        # fmt: off
        source_uri = "https://storage.cloud.google.com/{{ params['feeds_staging_gcs_daily_bucket'] }}/export_date={{ next_ds.replace('-', '') }}/type_1/{{ params['store_visits_daily_table'].replace('_','-') }}/historical_90/general/gzip_parquet_format/*"
        # fmt: on

        upload_to_blobs = BashOperator(
            task_id="upload_to_blobs",
            bash_command="{% include './include/bash/pby_azure_push/pby_azure_push.sh' %}",
            env={
                "ACCOUNT_NAME": account_name,
                "CONTAINER_NAME": container_name,
                "BLOB_NAME": "export_date={{ next_ds.replace('-', '') }}/{{ params['store_visits_daily_table'].replace('_','-') }}",  # Destination path
                "CLIENT_TOKEN_TXT_FILE_NAME": client_token_txt_file_name,
                "SOURCE_URI": source_uri,
                "GCS_KEYS_BUCKET": "{{ params['feeds_keys_gcs_bucket'] }}",
                "AZCOPY_CREDS_FILE_NAME": "{{ params['azure_pby_push_file_name'] }}",
            },
        )

        create_success_files = BashOperator(
            task_id="create_success_files",
            bash_command="{% include './include/bash/pby_azure_push/create_success.sh' %}",
            env={
                "ACCOUNT_NAME": account_name,
                "CONTAINER_NAME": container_name,
                "BLOB_NAME": "export_date={{ next_ds.replace('-', '') }}/_SUCCESS",  # Destination path
                "GCS_KEYS_BUCKET": "{{ params['feeds_keys_gcs_bucket'] }}",
                "AZCOPY_CREDS_FILE_NAME": "{{ params['azure_pby_push_file_name'] }}",
                "CLIENT_TOKEN_TXT_FILE_NAME": client_token_txt_file_name,
            },
        )

        start >> upload_to_blobs >> create_success_files

    return pby_azure_push_group
