from __future__ import annotations

from airflow.models import TaskInstance
from airflow.utils.task_group import TaskGroup

from common.operators.bigquery import OlvinBigQueryOperator
from common.operators.exceptions import MarkSuccessOperator
from common.utils import callbacks


def register(start: TaskInstance, dag: DAG) -> TaskGroup:
    """
    Register tasks on the dag

    Args:
        start (airflow.models.TaskInstance): Task instance all tasks will
        be registered downstream from.

    Returns:
        airflow.utils.task_group.TaskGroup: The task group in this section.
    """
    with TaskGroup(group_id="post-smc-demand") as group:
        migrate_prod_to_smc_demand = OlvinBigQueryOperator(
            task_id="migrate-smc-demand-to-prod",
            query="{% include './include/bigquery/post_smc/migrate-smc-demand-to-prod.sql' %}"
        )

        (
            start
            >> migrate_prod_to_smc_demand
        )

    return group
