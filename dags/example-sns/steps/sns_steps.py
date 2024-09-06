from __future__ import annotations

from airflow.models import TaskInstance
from airflow.utils.task_group import TaskGroup
from common.operators.bigquery import SNSBigQueryOperator


def register(start: TaskInstance, dag) -> TaskInstance:
    with TaskGroup(group_id="sns_steps") as group:
        sns_task_1 = SNSBigQueryOperator(
            task_id="sns_task_1",
            query="{% inlcude './include/bigquery/sns_task_1.sql' %}",
        )

        start >> sns_task_1

    return group
