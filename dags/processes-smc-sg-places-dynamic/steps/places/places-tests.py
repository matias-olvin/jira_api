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
    with TaskGroup(group_id="places-test") as group:
        test_duplicates = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="places-test-olvin-id-duplicates",
            query="{% include './include/bigquery/places/tests/test-duplicates.sql' %}",
        )
        test_nulls = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="places-test-olvin-id-nulls",
            query="{% include './include/bigquery/places/tests/test-nulls.sql' %}",
        )
        test_filter = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="places-test-olvin-id-filter",
            query="{% include './include/bigquery/places/tests/test-filter.sql' %}",
        )
        test_brands = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="places-test-olvin-id-brands",
            query="{% include './include/bigquery/places/tests/test-brands.sql' %}",
        )

        start >> [test_duplicates, test_nulls, test_filter, test_brands]

    return group
