from __future__ import annotations

import pendulum
from airflow.models import DAG, TaskInstance, Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup
from common.operators.exceptions import MarkSuccessOperator


def register(start: TaskInstance, dag: DAG) -> TaskInstance:
    """
    Register tasks on the dag

    Args:
        dag (airflow.models.DAG): DAG instance to register tasks on.
        start (airflow.models.TaskInstance): Task instance all tasks will
        be registered downstream from.

    Returns:
        airflow.models.TaskInstance: The last task node in this section.
    """

    with TaskGroup(group_id="trigger_series_smc_start_to_end_date") as group:
        # set start_date as pipeline execution_date
        start_date = pendulum.parse(Variable.get("smc_start_date"), format="YYYY-MM-DD")
        # set end_date as (airflow) date for today
        end_date = pendulum.parse(Variable.get("smc_end_date"), format="YYYY-MM-DD")
        # calculate timedelta between start and end
        delta = end_date - start_date

        prev_task = start
        trigger = start  # added to prevent error in Airflow

        for i in range(delta.days + 1):
            # for each day in range, get date and Trigger DagRun.
            day = (start_date.add(days=i)).format("YYYY-MM-DD")
            day_nodash = day.replace("-", "")
            trigger_date = f"{day}T03:00:00"

            trigger = TriggerDagRunOperator(
                task_id=f"trigger_{day_nodash}",
                trigger_dag_id=f"{dag.params['visits_daily_backfill_pipeline']}",
                execution_date=f"{trigger_date}",
                wait_for_completion=True,
                poke_interval=60 * 2,
            )
            prev_task >> trigger

            prev_task = trigger

        backfill_end = EmptyOperator(task_id="backfill_end")

        check_smc_backfill_end = MarkSuccessOperator(task_id="check_smc_backfill_end")

        trigger >> backfill_end >> check_smc_backfill_end

    return group
