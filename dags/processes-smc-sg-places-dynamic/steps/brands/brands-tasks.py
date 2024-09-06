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
        airflow.utils.task_group.TaskGroup: The last task node in this section.
    """
    with TaskGroup(group_id="brands-tasks") as group:
        migrate_history_to_sns = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="brands-migrate-history-to-sns",
            query="{% include './include/bigquery/brands/brands-migrate-history-to-sns.sql' %}",
        )
        create_dynamic = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="brands-create-dynamic",
            query="{% include './include/bigquery/brands/brands-create-dynamic.sql' %}",
        )
        update_dynamic = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="brands-update-dynamic",
            query="{% include './include/bigquery/brands/brands-update-dynamic.sql' %}",
        )
        create_olvin = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="brand-create-olvin",
            query="{% include './include/bigquery/brands/brands-create-olvin-brands-ids.sql' %}",
        )

        (
            start
            >> migrate_history_to_sns
            >> create_dynamic
            >> update_dynamic
            >> create_olvin
        )

    return group
