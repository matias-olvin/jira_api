"""
DAG ID: scaling_models_creation_trigger
"""
from airflow.models import TaskInstance
from airflow.utils.task_group import TaskGroup
from common.operators.bigquery import OlvinBigQueryOperator


def register(start: TaskInstance) -> TaskInstance:
    """
    Register tasks on the dag

    Args:
        start (airflow.models.TaskInstance): Task instance all tasks will
        be registered downstream from.

    Returns:
        airflow.models.TaskInstance: The last task node in this section.
    """
    with TaskGroup(group_id="brands-tests") as group:
        test_duplicates = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="brands-test-duplicates",
            query="{% include './include/bigquery/brands/tests/test-duplicates.sql' %}",
        )
        test_nulls = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="brands-test-nulls",
            query="{% include './include/bigquery/brands/tests/test-nulls.sql' %}",
        )

        start >> [test_duplicates, test_nulls]

    return group
