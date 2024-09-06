from airflow.models import TaskInstance
from airflow.operators.empty import EmptyOperator
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
    with TaskGroup(
        group_id="ingest_input_table_from_google_sheets_task_group"
    ) as group:

        create_snapshot_of_input_table = OlvinBigQueryOperator(
            task_id="create_snapshot_of_input_table",
            query="{% include './include/bigquery/places/ingest_input_table_from_google_sheets/create_snapshot_of_input_table.sql' %}",
        )

        check_input_data_in_google_sheets = MarkSuccessOperator(
            task_id="check_input_data_in_google_sheets",
            on_failure_callback=check_google_sheet_slack_alert(
                "U03BANPLXJR",  # CARLOS
                spreadsheet_id=spreadsheet_id,
                message=f"Check data in Google Spreadsheet under worksheet {worksheet_title}.",
            ),
        )

        source_dataset_table = "manually_add_pois.input_info"

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

        load_input_table_to_bigquery_from_gcs = OlvinBigQueryOperator(
            task_id="load_input_table_to_bigquery_from_gcs",
            query="{% include './include/bigquery/places/ingest_input_table_from_google_sheets/load_input_table_to_bigquery_from_gcs.sql' %}",
        )

        check_duplicates_in_input_info_table = OlvinBigQueryOperator(
            task_id="check_duplicates_in_input_info_table",
            query="{% include './include/bigquery/places/ingest_input_table_from_google_sheets/input_info_checks/check_duplicates_in_input_info_table.sql' %}",
        )

        check_nulls_fk_sgplaces = OlvinBigQueryOperator(
            task_id="check_nulls_fk_sgplaces",
            query="{% include './include/bigquery/places/ingest_input_table_from_google_sheets/input_info_checks/check_nulls_fk_sgplaces.sql' %}",
        )

        check_nulls_city_postal_code_polygon = OlvinBigQueryOperator(
            task_id="check_nulls_city_postal_code_polygon",
            query="{% include './include/bigquery/places/ingest_input_table_from_google_sheets/input_info_checks/check_nulls_city_postal_code_polygon.sql' %}",
        )

        check_unique_fk_sgbrands_per_brand = OlvinBigQueryOperator(
            task_id="check_unique_fk_sgbrands_per_brand",
            query="{% include './include/bigquery/places/ingest_input_table_from_google_sheets/input_info_checks/check_unique_fk_sgbrands_per_brand.sql' %}",
        )

        check_lat_lon_coords_with_region = OlvinBigQueryOperator(
            task_id="check_lat_lon_coords_with_region",
            query="{% include './include/bigquery/places/ingest_input_table_from_google_sheets/input_info_checks/check_lat_lon_coords_with_region.sql' %}",
        )

        verify_intended_region = OlvinBigQueryOperator(
            task_id="verify_intended_region",
            query="{% include './include/bigquery/places/ingest_input_table_from_google_sheets/input_info_checks/verify_intended_region.sql' %}",
        )

        input_table_ingestion_end = EmptyOperator(task_id="input_table_ingestion_end")

        (
            start
            >> create_snapshot_of_input_table
            >> check_input_data_in_google_sheets
            >> push_uris_to_xcom
            >> export_google_sheet_to_gcs_bucket
            >> load_input_table_to_bigquery_from_gcs
            >> [
                check_duplicates_in_input_info_table,
                check_nulls_fk_sgplaces,
                check_nulls_city_postal_code_polygon,
                check_unique_fk_sgbrands_per_brand,
                check_lat_lon_coords_with_region,
            ]
            >> verify_intended_region
            >> input_table_ingestion_end
        )

    return group
