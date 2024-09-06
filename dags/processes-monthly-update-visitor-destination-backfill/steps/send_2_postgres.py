"""
DAG ID: demographics_pipeline
"""
import csv
import datetime
import gzip
import logging
from tempfile import NamedTemporaryFile

from airflow.contrib.operators.gcs_delete_operator import (
    GoogleCloudStorageDeleteOperator,
)
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator


def register(dag, start):
    final_tasks = []

    def send_tables(dataset_bigquery: str, table_bigquery: str, database_table: str):
        # Export to GCS
        current_year = datetime.date.today().year
        lastMonth = (
            datetime.date.today().replace(day=1) - datetime.timedelta(days=1)
        ).month
        export_job = BigQueryInsertJobOperator(
            task_id=f"export_to_GCS_{database_table}",
            configuration={
                "extract": {
                    "destinationUris": (
                        (
                            f"gs://{{{{ params['staging_bucket'] }}}}/{database_table}/*.csv"
                        )
                    ),
                    "printHeader": True,
                    "destinationFormat": "CSV",
                    "fieldDelimiter": "\t",
                    # "compression": "GZIP",
                    "sourceTable": {
                        "projectId": dag.params["storage-prod"],
                        "datasetId": dataset_bigquery,
                        "tableId": table_bigquery,
                    },
                }
            },
            dag=dag,
        )
        start >> export_job
        # Delete possible matching data from database
        delete_operator = PostgresOperator(
            task_id=f"delete_{database_table}",
            dag=dag,
            postgres_conn_id=dag.params["postgres_conn_id"],
            sql=f"TRUNCATE TABLE {database_table.lower()} ",
        )
        export_job >> delete_operator

        # Upload new data to database
        def copy_to_gcs(bucket_name, gcs_folder, table_name):
            """Python function to copy files one by one form GCS and upload them to Postgres"""
            # Hooks with functions to connect to Postgres and GCS
            pg_hook = PostgresHook.get_hook(
                dag.params["postgres_conn_id"]
            )  # Name of the connection
            gcs_hook = GCSHook()
            # Get list of all CSV files in folder
            file_list = gcs_hook.list(bucket_name, prefix=gcs_folder, delimiter=".csv")
            # Upload one file at a time
            for file_name in file_list:
                # Create temporary file for data from GCS
                with NamedTemporaryFile(suffix=".csv") as temp_file:
                    temp_name = temp_file.name
                    logging.info("Downloading data from file '%s'", file_name)
                    gcs_hook.download(bucket_name, file_name, temp_name)
                    logging.info("Uploading data to table '%s'", table_name)
                    # Get list of headers so upload is done in order
                    with open(temp_name, "r") as f:
                        reader = csv.reader(f, delimiter="\t")
                        # The line is a list of keys
                        headers = ", ".join(next(reader))
                    # copy_expert avoids needing superuser permissions
                    pg_hook.copy_expert(
                        rf"COPY {table_name} ({headers}) FROM STDIN WITH CSV HEADER DELIMITER E'\t'",
                        temp_name,
                    )

        upload_operator = PythonOperator(
            task_id=f"upload_{database_table}",
            dag=dag,
            python_callable=copy_to_gcs,  # The function previously defined
            op_kwargs={
                "bucket_name": "{{ params['staging_bucket'] }}",  # Source bucket
                "gcs_folder": f"{database_table}",  # Source folder
                "table_name": f"{database_table.lower()}",  # Destination table
            },
        )
        delete_operator >> upload_operator

        # Delete staging data
        delete_staging = GoogleCloudStorageDeleteOperator(
            task_id=f"delete_staging_{database_table}",
            depends_on_past=False,
            dag=dag,
            bucket_name=dag.params["staging_bucket"],
            prefix=f"{database_table}/",
        )
        upload_operator >> delete_staging

        final_tasks.append(delete_staging)

    send_tables(
        dataset_bigquery=dag.params["postgres_dataset"],
        table_bigquery=dag.params["visitor_brand_dest_table"],
        database_table=dag.params["visitor_brand_dest_table"],
    )

    return final_tasks
