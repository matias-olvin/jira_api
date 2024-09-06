from __future__ import annotations

from airflow.models import TaskInstance
from airflow.operators.empty import EmptyOperator
from common.operators.bigquery import OlvinBigQueryOperator
from common.processes.google_sheets import (
    get_default_google_sheets_uris,
    google_sheet_task_group,
    push_default_google_sheets_default_uris_to_xcom_task,
)


def register(start: TaskInstance) -> TaskInstance:

    source_dataset_table = (
        "sg_base_tables_staging_dataset.naics_code_subcategories_table"
    )

    input_uri, output_uri = get_default_google_sheets_uris(
        source_dataset_table=source_dataset_table
    )

    create_snapshot_naics_code_subcategories = OlvinBigQueryOperator(
        task_id="create_snapshot_naics_code_subcategories",
        query="{% include './include/bigquery/naics_codes/snapshot_creations/create_snapshot_naics_code_subcategories.sql' %}",
    )

    default_uris_xcom_task = push_default_google_sheets_default_uris_to_xcom_task(
        task_id="uris_xcom_push_naics_code_subcategories_table",
        source_dataset_table=source_dataset_table,
    )

    export_naics_code_subcategories_table_to_gcs = OlvinBigQueryOperator(
        task_id="export_naics_code_subcategories_table_to_gcs",
        query="{% include './include/bigquery/naics_codes/update_naics_code_subcategories/export_naics_code_subcategories.sql' %}",
    )

    # https://docs.google.com/spreadsheets/d/18BMP1QhTWg7x_S7-HaFQsHicIhecLdocUGGzpb6DjZs/edit#gid=0
    group = google_sheet_task_group(
        start=export_naics_code_subcategories_table_to_gcs,
        spreadsheet_id="18BMP1QhTWg7x_S7-HaFQsHicIhecLdocUGGzpb6DjZs",
        worksheet_title="naics_code_subcats-check",
        source_dataset_table=source_dataset_table,
        input_uri=input_uri,
        output_uri=output_uri,
    )

    load_naics_code_subcategories_table_from_gcs = OlvinBigQueryOperator(
        task_id="load_naics_code_subcategories_table_from_gcs",
        query="{% include './include/bigquery/naics_codes/update_naics_code_subcategories/load_naics_code_subcategories.sql' %}",
    )

    test_duplicates = OlvinBigQueryOperator(
        task_id="naics_code_subcategories_table_test_duplicates",
        query="{% include './include/bigquery/naics_codes/update_naics_code_subcategories/tests/test-duplicates.sql' %}",
    )

    test_nulls = OlvinBigQueryOperator(
        task_id="naics_code_subcategories_table_test_nulls",
        query="{% include './include/bigquery/naics_codes/update_naics_code_subcategories/tests/test-nulls.sql' %}",
    )

    update_naics_code_subcategories_table_end = EmptyOperator(
        task_id="update_naics_code_subcategories_table_end"
    )

    (
        start
        >> create_snapshot_naics_code_subcategories
        >> default_uris_xcom_task
        >> export_naics_code_subcategories_table_to_gcs
        >> group
        >> load_naics_code_subcategories_table_from_gcs
        >> [test_duplicates, test_nulls]
        >> update_naics_code_subcategories_table_end
    )

    return update_naics_code_subcategories_table_end
