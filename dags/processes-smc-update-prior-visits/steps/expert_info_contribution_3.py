from airflow.models import TaskInstance
from common.operators.bigquery import OlvinBigQueryOperator
from common.processes.google_sheets import (
    get_default_google_sheets_uris,
    google_sheet_task_group,
    push_default_google_sheets_default_uris_to_xcom_task,
)


def register(
    start: TaskInstance, spreadsheet_id: str, worksheet_title: str, env: str
) -> TaskInstance:

    source_dataset_table = (
        "prior_brand_visits_dev_dataset.new_brands_table"
        if env == "dev"
        else "smc_ground_truth_volume_dataset.new_brands_table"
    )

    input_uri, output_uri = get_default_google_sheets_uris(
        source_dataset_table=source_dataset_table
    )

    create_snapshot_new_brands = OlvinBigQueryOperator(
        task_id="create_snapshot_new_brands",
        query="{% include './include/bigquery/snapshots/create_snapshot_new_brands.sql' %}",
    )

    default_uris_xcom_task = push_default_google_sheets_default_uris_to_xcom_task(
        task_id="uris_xcom_push_new_brands",
        source_dataset_table=source_dataset_table,
    )

    export_new_brands_to_gcs = OlvinBigQueryOperator(
        task_id="export_new_brands_to_gcs",
        query="{% include './include/bigquery/expert_info_contribution_3/export_new_brands_to_gcs.sql' %}",
    )

    group = google_sheet_task_group(
        start=export_new_brands_to_gcs,
        spreadsheet_id=spreadsheet_id,
        worksheet_title=worksheet_title,
        source_dataset_table=source_dataset_table,
        input_uri=input_uri,
        output_uri=output_uri,
    )

    load_new_brands_from_gcs = OlvinBigQueryOperator(
        task_id="load_new_brands_from_gcs",
        query="{% include './include/bigquery/expert_info_contribution_3/load_new_brands_from_gcs.sql' %}",
    )

    check_duplicates_in_new_brands = OlvinBigQueryOperator(
        task_id="check_duplicates_in_new_brands",
        query="{% include './include/bigquery/expert_info_contribution_3/checks/check_duplicates_in_new_brands.sql' %}",
    )

    check_nulls_in_new_brands = OlvinBigQueryOperator(
        task_id="check_nulls_in_new_brands",
        query="{% include './include/bigquery/expert_info_contribution_3/checks/check_nulls_in_new_brands.sql' %}",
    )

    append_new_brands_to_prior_brand_visits = OlvinBigQueryOperator(
        task_id="append_new_brands_to_prior_brand_visits",
        query="{% include './include/bigquery/expert_info_contribution_3/append_new_brands_to_prior_brand_visits.sql' %}",
    )

    (
        start
        >> create_snapshot_new_brands
        >> default_uris_xcom_task
        >> export_new_brands_to_gcs
        >> group
        >> load_new_brands_from_gcs
        >> [check_duplicates_in_new_brands, check_nulls_in_new_brands]
        >> append_new_brands_to_prior_brand_visits
    )

    return append_new_brands_to_prior_brand_visits
