from typing import Tuple

from airflow.models import TaskInstance, Variable
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from common.operators.bigquery import OlvinBigQueryOperator
from common.operators.exceptions import MarkSuccessOperator
from common.operators.google_sheets_connections import (
    GCSToGoogleSheetsOperator,
    GoogleSheetsToGCSOperator,
)
from common.utils.queries import max_row_count_check

INPUT_FOLDER = "google_sheets_input"
OUTPUT_FOLDER = "google_sheets_output"
bucket_name = (
    "pby-process-d-gcs-euwe1-sheets"
    if "dev" in Variable.get("env_project")
    else "pby-process-p-gcs-euwe1-sheets"
)


def push_default_google_sheets_default_uris_to_xcom_task(
    task_id: str, source_dataset_table: str
) -> TaskInstance:
    """
    Push the default google sheet uris to xcom.

    The uris are pushed to xcom so that they can be retrieved easily in EXPORT and LOAD table queries between GCS and BigQuery 
    (for the google sheets process). It also ensures that the objects in GCS are following the correct naming conventions.

    Args:
        task_id (str): A string to define the task_id of the airflow task
        source_dataset_table (str): A string in the format "dataset.table" specifying the BigQuery source table. Must be keys from config.yaml.

    Returns:
        Airflow TaskInstance

    Notes:
        - To obtain values via jinja templating in the EXPORT and LOAD queries respectively:\n
        `uri = {{ ti.xcom_pull(task_ids="task_id")["query_input_uri"] }}` and `uri = {{ ti.xcom_pull(task_ids="task_id")["query_output_uri"] }}`
    """
    def _push_value_to_xcom(dataset_table: str, **context) -> str:
        source_dataset, source_table = dataset_table.split(".")

        exec_date = context["ds"]

        input_uri = f"gs://{bucket_name}/{INPUT_FOLDER}/ingestion_date={exec_date}/{source_dataset}_{source_table}/*.csv.gz"

        output_uri = f"gs://{bucket_name}/{OUTPUT_FOLDER}/ingestion_date={exec_date}/{source_dataset}_{source_table}/{source_dataset}_{source_table}.csv.gz"

        return {"query_input_uri": input_uri, "query_output_uri": output_uri}

    push_google_sheet_uris_to_xcom = PythonOperator(
        task_id=task_id,
        python_callable=_push_value_to_xcom,
        provide_context=True,
        op_kwargs={"dataset_table": source_dataset_table},
    )

    return push_google_sheet_uris_to_xcom


def get_default_google_sheets_uris(
    source_dataset_table: str,
) -> Tuple[str, str]:
    """
    Get the default input and output uris for the google_sheet_task_group params input_uri and output_uri.

    Args:
        source_dataset_table (str): A string in the format "dataset.table" specifying the BigQuery source table. Must be keys from config.yaml.

    Returns:
        A Tuple containing input and output uri

        `input_uri, output_uri = get_default_google_sheets_uris(
    source_dataset_table="example_dataset.example_table"
    )`
    """
    source_dataset, source_table = source_dataset_table.split(".")

    input_uri = f"gs://{bucket_name}/{INPUT_FOLDER}/ingestion_date={{{{ ds }}}}/{source_dataset}_{source_table}/"

    output_uri = f"gs://{bucket_name}/{OUTPUT_FOLDER}/ingestion_date={{{{ ds }}}}/{source_dataset}_{source_table}/{source_dataset}_{source_table}.csv.gz"

    return input_uri, output_uri


def google_sheet_task_group(
    start: TaskInstance,
    spreadsheet_id: str,
    worksheet_title: str,
    source_dataset_table: str,
    input_uri: str,
    output_uri: str,
) -> TaskGroup:
    """
    An Airflow taskgroup that handles the loading and exporting from GCS to a Google Sheet visa versa

    Args:
        start (TaskInstance): The starting task in the DAG.
        spreadsheet_id (str): The Google Sheets spreadsheet ID. Found in the url https://docs.google.com/spreadsheets/d/SPREADSHEET_ID/edit
        worksheet_title (str): The title of the worksheet in the Google Sheets. For example "Sheet1".
        source_dataset_table (str): A string in the format "dataset.table" specifying the BigQuery source table. Must be keys from config.yaml.
        input_uri (str, optional): The URI for the input data in GCS.
        output_uri (str, optional): The URI for the output data in GCS.

    Returns:
        Airflow TaskGroup
    """
    source_dataset, source_table = source_dataset_table.split(".")

    with TaskGroup(
        group_id=f"review_{source_dataset}_{source_table}_via_google_sheets"
    ) as group:

        print(f"Using input uri: {input_uri}")
        print(f"Using output uri:  {output_uri}")

        source_table_string = f"{{{{ var.value.env_project }}}}.{{{{ params['{source_dataset}'] }}}}.{{{{ params['{source_table}'] }}}}"

        # Define tasks for exporting data to GCS, checking row count, and updating Google Sheets
        check_row_count_task = OlvinBigQueryOperator(
            task_id=f"check_row_count_for_{source_dataset}-{source_table}",
            query=max_row_count_check(table=source_table_string, max_row_count=40000),
        )

        update_spreadsheet_with_bq_table = GCSToGoogleSheetsOperator(
            task_id=f"update_spreadsheet_with_{source_dataset}_{source_table}",
            uri=input_uri,
            spreadsheet_id=spreadsheet_id,  # https://docs.google.com/spreadsheets/d/SPREADSHEET_ID/edit
            worksheet_title=worksheet_title,  # eg Sheet1
        )

        # Task to give operators a chance to make changes to google sheet (if required)
        mark_success_to_confirm_google_sheet_changes = MarkSuccessOperator(
            task_id="mark_success_to_confirm_google_sheet_changes"
        )

        export_spreadsheet_to_gcs_single = GoogleSheetsToGCSOperator(
            task_id="export_spreadsheet_to_gcs",
            uri=output_uri,
            spreadsheet_id=spreadsheet_id,
            worksheet_title=worksheet_title,
        )

        (
            start
            >> check_row_count_task
            >> update_spreadsheet_with_bq_table
            >> mark_success_to_confirm_google_sheet_changes
            >> export_spreadsheet_to_gcs_single
        )

    return group
