from __future__ import annotations

from airflow.exceptions import AirflowSkipException
from airflow.models import TaskInstance
from airflow.operators.python import PythonOperator
from common.operators.bigquery import OlvinBigQueryOperator

ENDPOINT = "reviews"
PREFIX = "reviews-transform-output"


def skip_static_features(**context) -> None:
    max_row_num = int(
        context["ti"].xcom_pull(task_ids="max_row_num", key="max_row_num")
    )
    batch_size = int(context["dag_run"].conf["batch-size"])
    final_batch = -(max_row_num // -batch_size)  # ceil by reversing floor
    batch_number = int(context["dag_run"].conf["batch-number"])

    if batch_number != final_batch:
        raise AirflowSkipException()


def register(start: TaskInstance) -> TaskInstance:
    load = OlvinBigQueryOperator(
        task_id="data-load",
        query="{% include './include/bigquery/data-load.sql' %}",
        params={
            "ENDPOINT": ENDPOINT,
            "PREFIX": PREFIX,
        },
    )
    static_features_history = OlvinBigQueryOperator(
        task_id="static-features-history",
        query="{% include './include/bigquery/static-features-history.sql' %}",
    )
    run_static_features = PythonOperator(
        task_id="run-static-features",
        python_callable=skip_static_features,
    )
    static_features = OlvinBigQueryOperator(
        task_id="static-features",
        query="{% include './include/bigquery/static-features.sql' %}",
    )
    static_features_metrics = OlvinBigQueryOperator(
        task_id="static-features-metrics",
        query="{% include './include/bigquery/static-features-metrics.sql' %}",
    )

    (
        start
        >> load
        >> static_features_history
        >> run_static_features
        >> static_features
        >> static_features_metrics
    )

    return static_features_metrics
