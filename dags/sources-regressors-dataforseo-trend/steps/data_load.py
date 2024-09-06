from __future__ import annotations

from airflow.models import TaskInstance
from airflow.utils.task_group import TaskGroup
from common.operators.bigquery import OlvinBigQueryOperator


def register(start: TaskInstance) -> TaskGroup:
    load = OlvinBigQueryOperator(
        task_id="data-load",
        query="{% include './include/bigquery/data-load.sql' %}",
        params={
            "ENDPOINT": "trend",
            "PREFIX": "trend-transform-output",
        },
    )

    with TaskGroup(group_id="regressor") as group:
        collection = OlvinBigQueryOperator(
            task_id="collection",
            query="{% include './include/bigquery/regressor-collection.sql' %}",
            params={
                "ENDPOINT": "trend",
                "PREFIX": "trend-transform-output",
            },
        )
        dictionary = OlvinBigQueryOperator(
            task_id="dictionary",
            query="{% include './include/bigquery/regressor-dictionary.sql' %}",
            params={
                "ENDPOINT": "trend",
                "PREFIX": "trend-transform-output",
            },
        )

    start >> load >> group

    return group
