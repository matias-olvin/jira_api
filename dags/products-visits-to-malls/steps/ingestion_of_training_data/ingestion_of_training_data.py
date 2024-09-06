from __future__ import annotations

from airflow.models import TaskInstance
from airflow.utils.task_group import TaskGroup
from common.operators.bigquery import OlvinBigQueryOperator
from common.operators.exceptions import MarkSuccessOperator
from common.operators.google_sheets_connections import GoogleSheetsToGCSOperator
from common.processes.google_sheets import (
    get_default_google_sheets_uris,
    push_default_google_sheets_default_uris_to_xcom_task,
)
from common.utils.callbacks import check_google_sheet_slack_alert


def register(
    start: TaskInstance, spreadsheet_id: str, worksheet_title: str
) -> TaskGroup:

    with TaskGroup(group_id="ingest_training_data_from_google_sheets") as group:

        check_training_data_in_google_sheets = MarkSuccessOperator(
            task_id="check_training_data_in_google_sheets",
            on_failure_callback=check_google_sheet_slack_alert(
                "U05FN3F961X",  # IGNACIO
                "U05M60N8DMX",  # MATIAS
                spreadsheet_id=spreadsheet_id,
                message=f"Check data under worksheet {worksheet_title}.",
            ),
        )

        source_dataset_table = (
            "visits_to_malls_dataset.visits_to_malls_training_data_table"
        )

        push_uris_to_xcom = push_default_google_sheets_default_uris_to_xcom_task(
            task_id="push_uris_to_xcom", source_dataset_table=source_dataset_table
        )

        _, output_uri = get_default_google_sheets_uris(
            source_dataset_table=source_dataset_table
        )

        export_google_sheet_to_gcs_bucket = GoogleSheetsToGCSOperator(
            task_id="export_google_sheet_to_gcs_bucket",
            uri=output_uri,
            spreadsheet_id=spreadsheet_id,
            worksheet_title=worksheet_title,
        )

        load_training_data_to_staging = OlvinBigQueryOperator(
            task_id="load_training_data_to_staging",
            query="{% include './include/bigquery/ingestion_of_training_data/load_training_data_to_staging.sql' %}",
        )

        check_duplicates_in_training_data_table = OlvinBigQueryOperator(
            task_id="check_duplicates_in_training_data_table",
            query="{% include './include/bigquery/ingestion_of_training_data/check_duplicates_in_training_data_table.sql' %}",
        )

        check_nulls_in_training_data_table = OlvinBigQueryOperator(
            task_id="check_nulls_in_training_data_table",
            query="{% include './include/bigquery/ingestion_of_training_data/check_nulls_in_training_data_table.sql' %}",
        )

        create_snapshot_of_training_data = OlvinBigQueryOperator(
            task_id="create_snapshot_of_training_data",
            query="{% include './include/bigquery/snapshots/create_snapshot_of_training_data.sql' %}",
        )

        copy_training_data_from_staging = OlvinBigQueryOperator(
            task_id="copy_training_data_from_staging",
            query="{% include './include/bigquery/ingestion_of_training_data/copy_training_data_from_staging.sql' %}",
        )

        (
            start
            >> check_training_data_in_google_sheets
            >> push_uris_to_xcom
            >> export_google_sheet_to_gcs_bucket
            >> load_training_data_to_staging
            >> [
                check_duplicates_in_training_data_table,
                check_nulls_in_training_data_table,
            ]
            >> create_snapshot_of_training_data
            >> copy_training_data_from_staging
        )

        return group
