from __future__ import annotations

from airflow.exceptions import AirflowSkipException
from airflow.models import TaskInstance
from airflow.operators.python import BranchPythonOperator, PythonOperator
from common.operators.bigquery import OlvinBigQueryOperator
from google.cloud import bigquery

ENDPOINT = "reviews"
PREFIX = "reviews-post-input"


def data_prep_branch(**context) -> str:
    branch_number = int(context["dag_run"].conf["batch-number"])
    trigger = context["dag_run"].conf["trigger"]
    if branch_number == 1:
        if trigger == "partial":
            return "data-prep-partial"
        return "data-prep-full"
    else:
        raise AirflowSkipException()


def max_row_num(project: str, dataset: str, table: str, **context) -> None:
    client = bigquery.Client()
    query = (
        "SELECT MAX(row_num)\n "
        f"FROM `{project}.{dataset}.{table}`"
    )
    job = client.query(query)
    result = [row[0] for row in job.result()][0]

    context["ti"].xcom_push(key="max_row_num", value=result)


def register(start: TaskInstance) -> TaskInstance:
    prep_branch = BranchPythonOperator(
        task_id="data-prep-branch",
        python_callable=data_prep_branch,
        provide_context=True,
    )

    prep_full = OlvinBigQueryOperator(
        task_id="data-prep-full",
        query="{% include './include/bigquery/data-prep-full.sql' %}",
    )
    prep_partial = OlvinBigQueryOperator(
        task_id="data-prep-partial",
        query="{% include './include/bigquery/data-prep-partial.sql' %}",
    )
    rows = PythonOperator(
        task_id="max_row_num",
        python_callable=max_row_num,
        op_kwargs={
            "project": "{{ var.value.env_project }}",
            "dataset": "{{ params['static_features_staging_dataset'] }}",
            "table": "{{ params['sgplaceraw_table'] }}",
        },
        trigger_rule="none_failed",
    )
    export = OlvinBigQueryOperator(
        task_id="data-export",
        query="{% include './include/bigquery/data-export.sql' %}",
        params={
            "ENDPOINT": ENDPOINT,
            "PREFIX": PREFIX,
        },
        trigger_rule="none_failed",
    )

    start >> prep_branch >> [prep_full, prep_partial] >> rows >> export

    return export
