"""
DAG ID: demographics_pipeline
"""
import csv
import logging
from tempfile import NamedTemporaryFile

import pandas as pd
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.providers.google.cloud.transfers.bigquery_to_bigquery import (
    BigQueryToBigQueryOperator,
)
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator


def register(dag, start):
    """Register tasks on the dag.

    Args:
        dag (airflow.models.DAG): DAG instance to register tasks on.
        start (airflow.models.TaskInstance): Task instance all tasks will
            registered downstream from.

    Returns:
        airflow.models.TaskInstance: The last task node in this section.
    """
    move_to_postgres_bq_tasks = []
    move_to_postgres_bq_dummy = DummyOperator(
        task_id="move_to_postgres_bq_dummy", dag=dag
    )

    complete_move_to_postgres_bq = DummyOperator(
        task_id="complete_move_to_postgres_bq", dag=dag
    )

    truncate_large_staging = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="truncate_large_staging",
        # project_id=Variable.get("compute_project_id"),
        configuration={
            "query": {
                "query": "{% include './bigquery/postgres/truncate_staging.sql' %}",
                "useLegacySql": "False",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id |  replace('.','-') }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )

    def move_table_to_postgres_bq(
        dataset_param: str, table_param: str, postgres_table_param: str
    ):
        copy_table = BigQueryToBigQueryOperator(
            task_id=f"copy_table_{dataset_param}_{table_param}",
            source_project_dataset_tables="{{ params['project'] }}."
            + f"{{{{ params['{dataset_param}'] }}}}.{{{{ params['{table_param}'] }}}}",
            destination_project_dataset_table="{{ params['project'] }}.postgres."
            + f"{postgres_table_param}",
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_TRUNCATE",
            dag=dag,
        )
        move_to_postgres_bq_tasks.append(copy_table)

    move_table_to_postgres_bq(
        "visits_estimation_dataset",
        "SGPlaceHourlyVisitsRaw_table",
        "SGPlaceHourlyVisitsRaw",
    )
    move_table_to_postgres_bq(
        "visits_estimation_dataset",
        "SGPlaceHourlyAllVisitsRaw_table",
        "SGPlaceHourlyAllVisitsRaw",
    )
    move_table_to_postgres_bq(
        "visits_estimation_dataset",
        "SGPlaceDailyVisitsRaw_table",
        "SGPlaceDailyVisitsRaw",
    )
    move_table_to_postgres_bq(
        "visits_estimation_dataset",
        "ZipCodeHourlyVisitsRaw_table",
        "ZipCodeHourlyVisitsRaw",
    )
    move_table_to_postgres_bq(
        "visits_estimation_dataset",
        "ZipCodeDailyVisitsRaw_table",
        "ZipCodeDailyVisitsRaw",
    )
    move_table_to_postgres_bq(
        "visits_estimation_dataset",
        "SGPlaceMonthlyVisitsRaw_table",
        "SGPlaceMonthlyVisitsRaw",
    )
    move_table_to_postgres_bq(
        "visits_estimation_dataset", "SGPlaceRanking_table", "SGPlaceRanking"
    )
    move_table_to_postgres_bq(
        "visits_estimation_dataset",
        "SGPlacePatternVisitsRaw_table",
        "SGPlacePatternVisitsRaw",
    )

    (
        start
        >> truncate_large_staging
        >> move_to_postgres_bq_dummy
        >> move_to_postgres_bq_tasks
        >> complete_move_to_postgres_bq
    )

    return complete_move_to_postgres_bq
