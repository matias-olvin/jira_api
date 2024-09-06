from __future__ import annotations

from airflow.models import DAG, TaskInstance, Variable
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.utils.task_group import TaskGroup
from common.operators.bigquery import OlvinBigQueryOperator


def register(start: TaskInstance, dag: DAG, env: str) -> TaskGroup:
    """
    Register tasks on the dag

    Args:
        start (airflow.models.TaskInstance): Task instance all tasks will
        be registered downstream from.
        dag (airflow.models.DAG): The dag object.
        env (str): The environment the dag is running in.

    Returns:
        airflow.utils.task_group.TaskGroup: The task group in this section.
    """

    def branch_task_first_monday_fn(**context):
        """
        If the run date is the first Monday of the month, then return the task ID of the task that
        clears the historical table. Otherwise, return the task ID of the task that clears the weekly
        table.
        :return: The name of the task to be executed.
        """
        run_date = context["next_ds"]
        first_monday = Variable.get("first_monday")
        return (
            "visionworks.clear_gcs_placekeys_monthly"
            if run_date == first_monday
            else "visionworks.end"
        )

    with TaskGroup(group_id="visionworks") as group:

        branch_task_first_monday = BranchPythonOperator(
            task_id="branch_task_first_monday",
            provide_context=True,
            python_callable=branch_task_first_monday_fn,
        )

        start >> branch_task_first_monday

        clear_gcs_placekeys_monthly = GCSDeleteObjectsOperator(
            task_id="clear_gcs_placekeys_monthly",
            bucket_name="{{ params['feeds_staging_gcs_no_pays_bucket'] }}",
            prefix="visionworks/export_date={{ next_ds.replace('-', '') }}/{{ params['placekeys_table'].replace('_','-') }}/",
        )

        clear_gcs_store_visitors_monthly = GCSDeleteObjectsOperator(
            task_id="clear_gcs_store_visitors_monthly",
            bucket_name="{{ params['feeds_staging_gcs_no_pays_bucket'] }}",
            prefix="visionworks/export_date={{ next_ds.replace('-', '') }}/{{ params['store_visitors_table'].replace('_','-') }}/",
        )

        clear_gcs_store_visits_trend_monthly = GCSDeleteObjectsOperator(
            task_id="clear_gcs_store_visits_trend_monthly",
            bucket_name="{{ params['feeds_staging_gcs_no_pays_bucket'] }}",
            prefix="visionworks/export_date={{ next_ds.replace('-', '') }}/{{ params['store_visits_trend_table'].replace('_','-') }}/",
        )

        export_placekeys_monthly = OlvinBigQueryOperator(
            task_id="export_placekeys_monthly",
            query="{% include './bigquery/visionworks/placekeys_monthly.sql' %}",
        )

        export_store_visitors_monthly = OlvinBigQueryOperator(
            task_id="export_store_visitors_monthly",
            query="{% include './bigquery/visionworks/store_visitors_monthly.sql' %}",
            billing_tier="high",
        )

        export_store_visits_trend_monthly = OlvinBigQueryOperator(
            task_id="export_store_visits_trend_monthly",
            query="{% include './bigquery/visionworks/store_visits_trend_monthly.sql' %}",
        )

        account_name = "edpprodscusdl2"
        container_name = "pass-by-incoming"
        client_token_txt_file_name = "visionworks.txt"
        # fmt: off
        source_uri = "https://storage.cloud.google.com/{{ params['feeds_staging_gcs_no_pays_bucket'] }}/visionworks/export_date={{ next_ds.replace('-', '') }}/*"
        # fmt: on

        upload_to_blobs = BashOperator(
            task_id="upload_to_blobs",
            bash_command="{% include './bash/visionworks/pby_azure_push.sh' %}",
            env={
                "ACCOUNT_NAME": account_name,
                "CONTAINER_NAME": container_name,
                "BLOB_NAME": "export_date={{ next_ds.replace('-', '') }}",  # Destination path
                "CLIENT_TOKEN_TXT_FILE_NAME": client_token_txt_file_name,
                "SOURCE_URI": source_uri,
                "GCS_KEYS_BUCKET": "{{ params['feeds_keys_gcs_bucket'] }}",
                "AZCOPY_CREDS_FILE_NAME": "{{ params['azure_pby_push_file_name'] }}",
            },
        )

        end = EmptyOperator(
            task_id="end", depends_on_past=False, trigger_rule="all_done"
        )

        (
            branch_task_first_monday
            >> clear_gcs_placekeys_monthly
            >> export_placekeys_monthly
            >> [
                clear_gcs_store_visitors_monthly,
                clear_gcs_store_visits_trend_monthly,
            ]
        )
        clear_gcs_store_visitors_monthly >> export_store_visitors_monthly
        clear_gcs_store_visits_trend_monthly >> export_store_visits_trend_monthly

        (
            [
                export_store_visitors_monthly,
                export_store_visits_trend_monthly,
            ]
            >> upload_to_blobs
            >> end
        )
        (branch_task_first_monday >> end)

    return group
