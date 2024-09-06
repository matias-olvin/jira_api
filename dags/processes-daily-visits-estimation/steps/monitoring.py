from __future__ import annotations

from airflow.models import DAG, TaskInstance, Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryValueCheckOperator
from airflow.utils.task_group import TaskGroup
from common.operators.bigquery import OlvinBigQueryOperator, SNSBigQueryOperator

def register(start: TaskInstance, dag: DAG) -> TaskGroup:
    BIGQUERY_PROJECT = (
        Variable.get("env_project")
        if dag.params["stage"] == "production"
        else Variable.get("almanac_project")
    )

    with TaskGroup(group_id="monitoring") as group:
        logs = SNSBigQueryOperator(
            task_id="factor",
            query="{% include './include/bigquery/monitoring/logs.sql' %}",
        )

    start >> group

    return group