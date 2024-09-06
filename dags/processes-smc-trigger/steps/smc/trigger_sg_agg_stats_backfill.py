from datetime import datetime, timedelta

from airflow.models import DAG, TaskInstance
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
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

    def exec_delta_fn(execution_date):
        """
        > It takes the execution date and returns the execution date minus the difference between the
        execution date and the execution date with the time set to 00:00:00

        :param execution_date: The execution date of the task instance
        :return: The execution date minus the difference in seconds between the source and target
        execution dates.
        """
        source_exec_date = execution_date.replace(tzinfo=None)

        target_exec_date = source_exec_date.strftime("%Y-%m-%d")
        target_exec_date = datetime.strptime(f"{target_exec_date}", "%Y-%m-%d")

        diff = source_exec_date - target_exec_date
        diff_seconds = diff.total_seconds()

        return execution_date - timedelta(seconds=diff_seconds)

    with TaskGroup(group_id="trigger_sg_agg_stats_backfill") as group:
        # Triggering the sg_agg_stats_backfill_pipeline.
        trigger = TriggerDagRunOperator(
            task_id=f"trigger_{dag.params['sg_agg_stats_backfill_pipeline']}",
            trigger_dag_id="{{ params['sg_agg_stats_backfill_pipeline'] }}",
            execution_date=f"{datetime.today().strftime('%Y-%m-%d')}",
            reset_dag_run=True,
        )
        # Waiting for the sg_agg_stats_backfill_pipeline to finish.
        sensor = ExternalTaskSensor(
            task_id=f"{dag.params['sg_agg_stats_backfill_pipeline']}_sensor",
            external_dag_id=f"{dag.params['sg_agg_stats_backfill_pipeline']}",
            poke_interval=60 * 60,
            mode="reschedule",
            timeout=60 * 60 * 24 * 30,
            execution_date_fn=exec_delta_fn,
        )

        task_to_fail = MarkSuccessOperator(
            task_id="check_sg_agg_stats_backfill_end",
        )

        start >> trigger >> sensor >> task_to_fail

    return group
