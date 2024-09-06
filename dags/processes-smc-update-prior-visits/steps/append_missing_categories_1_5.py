from __future__ import annotations

from airflow.models import TaskInstance
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from common.operators.bigquery import OlvinBigQueryOperator
from common.operators.google_sheets_connections import GoogleSheetsToGCSOperator
from common.processes.google_sheets import (
    get_default_google_sheets_uris,
    push_default_google_sheets_default_uris_to_xcom_task,
)
from common.utils.callbacks import check_google_sheet_slack_alert


def register(
    start: TaskInstance, spreadsheet_id: str, worksheet_title: str, env: str
) -> TaskGroup:

    source_dataset_table = (
        "prior_brand_visits_dev_dataset.categories_to_append_table"
        if env == "dev"
        else "smc_ground_truth_volume_dataset.categories_to_append_table"
    )

    with TaskGroup(group_id="append_missing_categories_task_group") as group:

        _, output_uri = get_default_google_sheets_uris(
            source_dataset_table=source_dataset_table
        )

        default_uris_xcom_task = push_default_google_sheets_default_uris_to_xcom_task(
            task_id="uris_xcom_push_categories_to_append",
            source_dataset_table=source_dataset_table,
        )

        check_missing_categories = OlvinBigQueryOperator(
            task_id="check_missing_categories",
            query="{% include './include/bigquery/append_missing_categories_1_5/check_missing_categories.sql' %}",
            on_failure_callback=check_google_sheet_slack_alert(
                "U05FN3F961X",
                "U03BANPLXJR",
                spreadsheet_id=spreadsheet_id,
                message=f"Add values to missing categories that were obtained from this query under worksheet: {worksheet_title}",
            ),  # IGNACIO, CARLOS
        )

        export_google_sheet_to_gcs_bucket = GoogleSheetsToGCSOperator(
            task_id="export_google_sheet_to_gcs_bucket",
            uri=output_uri,
            spreadsheet_id=spreadsheet_id,
            worksheet_title=worksheet_title,
        )

        load_categories_to_append_from_gcs = OlvinBigQueryOperator(
            task_id="load_categories_to_append_from_gcs",
            query="{% include './include/bigquery/append_missing_categories_1_5/load_categories_to_append_from_gcs.sql' %}",
        )

        categories_to_append_test_duplicates = OlvinBigQueryOperator(
            task_id="categories_to_append_test_duplicates",
            query="{% include './include/bigquery/append_missing_categories_1_5/tests/categories_to_append_test_duplicates.sql' %}",
        )

        categories_to_append_test_nulls = OlvinBigQueryOperator(
            task_id="categories_to_append_test_nulls",
            query="{% include './include/bigquery/append_missing_categories_1_5/tests/categories_to_append_test_nulls.sql' %}",
        )

        create_categories_to_append_end = EmptyOperator(
            task_id="create_categories_to_append_end"
        )

        (
            start
            >> default_uris_xcom_task
            >> check_missing_categories
            >> export_google_sheet_to_gcs_bucket
            >> load_categories_to_append_from_gcs
            >> [categories_to_append_test_duplicates, categories_to_append_test_nulls]
            >> create_categories_to_append_end
        )

    return group
