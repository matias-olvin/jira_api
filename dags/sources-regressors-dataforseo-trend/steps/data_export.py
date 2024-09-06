from __future__ import annotations

from airflow.models import TaskInstance
from common.operators.bigquery import OlvinBigQueryOperator

ENDPOINT = "trend"
PREFIX = "trend-post-input"


def register(start: TaskInstance) -> TaskInstance:
    export = OlvinBigQueryOperator(
        task_id="data-export",
        query="{% include './include/bigquery/data-export.sql' %}",
        params={
            "ENDPOINT": ENDPOINT,
            "PREFIX": PREFIX,
        },
    )
    start >> export

    return export
