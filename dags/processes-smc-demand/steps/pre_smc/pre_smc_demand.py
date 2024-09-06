from __future__ import annotations

from airflow.models import TaskInstance, DAG
from airflow.utils.task_group import TaskGroup

from common.operators.bigquery import OlvinBigQueryOperator
from common.operators.exceptions import MarkSuccessOperator
from common.utils import callbacks, slack_users


def register(start: TaskInstance, dag: DAG) -> TaskGroup:
    """
    Register tasks on the dag

    Args:
        start (airflow.models.TaskInstance): Task instance all tasks will
        be registered downstream from.

    Returns:
        airflow.utils.task_group.TaskGroup: The task group in this section.
    """
    with TaskGroup(group_id="pre-smc-demand") as group:
        migrate_prod_to_smc_demand = OlvinBigQueryOperator(
            task_id="migrate-prod-to-smc-demand",
            query="{% include './include/bigquery/pre_smc/migrate-prod-to-smc-demand.sql' %}"
        )

        update_places_smc_demand_config = MarkSuccessOperator(
            task_id="update_places_smc_demand_config",
            on_failure_callback=callbacks.task_fail_slack_alert
            (
                "U034YDXAD1R",
                "U03BANPLXJR",
                slack_users.MATIAS,
                channel="prod"
            ),
        )
        update_smc_demand_filter = OlvinBigQueryOperator(
            task_id="update-smc-demand-filter",
            query="{% include './include/bigquery/pre_smc/update-smc-demand-filter.sql' %}"
        )

        (
            start
            >> update_places_smc_demand_config
            >> update_smc_demand_filter
            >> migrate_prod_to_smc_demand
        )

    return group
