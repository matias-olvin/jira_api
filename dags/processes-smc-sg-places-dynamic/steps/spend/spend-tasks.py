from __future__ import annotations

from airflow.models import TaskInstance
from airflow.utils.task_group import TaskGroup
from common.operators.bigquery import OlvinBigQueryOperator


def register(start: TaskInstance) -> TaskGroup:
    """
    Register tasks on the dag

    Args:
        start (airflow.models.TaskInstance): Task instance all tasks will
        be registered downstream from.

    Returns:
        airflow.utils.task_group.TaskGroup: The task group in this section.
    """
    with TaskGroup(group_id="spend-tasks") as group:
        update_spend_patterns = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="spend-create",
            query="{% include './include/bigquery/spend/spend-create.sql' %}",
        )

        start >> update_spend_patterns

    return group
