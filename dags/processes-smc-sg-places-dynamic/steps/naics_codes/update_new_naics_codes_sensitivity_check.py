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
        "sg_base_tables_staging_dataset.new_naics_codes_sensitivity_check_table"
    )

    input_uri, output_uri = get_default_google_sheets_uris(
        source_dataset_table=source_dataset_table
    )

    create_snapshot_new_naics_codes_sensitivity_check = OlvinBigQueryOperator(
        task_id="create_snapshot_new_naics_codes_sensitivity_check",
        query="{% include './include/bigquery/naics_codes/snapshot_creations/create_snapshot_new_naics_codes_sensitivity_check.sql' %}",
    )

    default_uris_xcom_task = push_default_google_sheets_default_uris_to_xcom_task(
        task_id="uris_xcom_push_new_naics_codes_sensitivity_check_table",
        source_dataset_table=source_dataset_table,
    )

    export_new_naics_codes_sensitivity_check_to_gcs = OlvinBigQueryOperator(
        task_id="export_new_naics_codes_sensitivity_check_to_gcs",
        query="{% include './include/bigquery/naics_codes/update_new_naics_codes_sensitivity_check/export_new_naics_codes_sensitivity_check_to_gcs.sql' %}",
    )

    # https://docs.google.com/spreadsheets/d/18BMP1QhTWg7x_S7-HaFQsHicIhecLdocUGGzpb6DjZs/edit#gid=0
    group = google_sheet_task_group(
        start=export_new_naics_codes_sensitivity_check_to_gcs,
        spreadsheet_id="18BMP1QhTWg7x_S7-HaFQsHicIhecLdocUGGzpb6DjZs",
        worksheet_title="new_naics_codes_sensitivity_check-check_naics",
        source_dataset_table=source_dataset_table,
        input_uri=input_uri,
        output_uri=output_uri,
    )

    load_new_naics_codes_sensitivity_check_from_gcs = OlvinBigQueryOperator(
        task_id="load_new_naics_codes_sensitivity_check_from_gcs",
        query="{% include './include/bigquery/naics_codes/update_new_naics_codes_sensitivity_check/load_new_naics_codes_sensitivity_check_from_gcs.sql' %}",
    )

    test_duplicates = OlvinBigQueryOperator(
        task_id="new_naics_codes_sensitivity_check_test_duplicates",
        query="{% include './include/bigquery/naics_codes/update_new_naics_codes_sensitivity_check/tests/test-duplicates.sql' %}",
    )

    test_nulls = OlvinBigQueryOperator(
        task_id="new_naics_codes_sensitivity_check_test_nulls",
        query="{% include './include/bigquery/naics_codes/update_new_naics_codes_sensitivity_check/tests/test-nulls.sql' %}",
    )

    create_snapshot_sensitive_naics_codes = OlvinBigQueryOperator(
        task_id="create_snapshot_sensitive_naics_codes",
        query="{% include './include/bigquery/naics_codes/snapshot_creations/create_snapshot_sensitive_naics_codes.sql' %}",
    )

    create_snapshot_non_sensitive_naics_codes = OlvinBigQueryOperator(
        task_id="create_snapshot_non_sensitive_naics_codes",
        query="{% include './include/bigquery/naics_codes/snapshot_creations/create_snapshot_non_sensitive_naics_codes.sql' %}",
    )

    update_sensitive_naics_codes = OlvinBigQueryOperator(
        task_id="update_sensitive_naics_codes",
        query="{% include './include/bigquery/naics_codes/update_new_naics_codes_sensitivity_check/update_sensitive_naics_codes.sql' %}",
    )

    update_non_sensitive_naics_codes = OlvinBigQueryOperator(
        task_id="update_non_sensitive_naics_codes",
        query="{% include './include/bigquery/naics_codes/update_new_naics_codes_sensitivity_check/update_non_sensitive_naics_codes.sql' %}",
    )

    new_naics_codes_sensitivity_check_end = EmptyOperator(
        task_id="new_naics_codes_sensitivity_check_end"
    )

    (
        start
        >> create_snapshot_new_naics_codes_sensitivity_check
        >> default_uris_xcom_task
        >> export_new_naics_codes_sensitivity_check_to_gcs
        >> group
        >> load_new_naics_codes_sensitivity_check_from_gcs
        >> [test_duplicates, test_nulls]
        >> create_snapshot_non_sensitive_naics_codes
        >> create_snapshot_sensitive_naics_codes
        >> update_sensitive_naics_codes
        >> update_non_sensitive_naics_codes
        >> new_naics_codes_sensitivity_check_end
    )

    return new_naics_codes_sensitivity_check_end
