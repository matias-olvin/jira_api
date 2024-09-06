from datetime import datetime, timedelta

from airflow.models import DAG, TaskInstance
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup


def register(start: TaskInstance, dag: DAG) -> TaskGroup:
    with TaskGroup(group_id="manual_poi_adjustments_task_group") as group:

        def exec_delta_fn(execution_date):
            """
            > It takes the execution date and returns the execution date minus the difference between the
            execution date and the execution date with the time set to 00:00:00

            :param execution_date: The execution date of the task instance
            :return: The execution date minus the difference in seconds between the source and target
            execution dates.
            """
            source_exec_date = execution_date.replace(tzinfo=None)

            target_exec_date = source_exec_date.strftime("%Y-%m-%dT00:00:00")
            target_exec_date = datetime.strptime(
                f"{target_exec_date}", "%Y-%m-%dT%H:%M:%S"
            )

            diff = source_exec_date - target_exec_date
            diff_seconds = diff.total_seconds()

            return execution_date - timedelta(seconds=diff_seconds)

        trigger_child_dag = TriggerDagRunOperator(
            task_id=f"trigger_{dag.params['dag-processes-monthly-update-manually-add-pois']}",
            trigger_dag_id=dag.params['dag-processes-monthly-update-manually-add-pois'],
            execution_date="{{ ds }}T00:00:00+00:00",
            reset_dag_run=True,
        )

        child_dag_sensor = ExternalTaskSensor(
            task_id=f"{dag.params['dag-processes-monthly-update-manually-add-pois']}_sensor",
            external_dag_id=dag.params['dag-processes-monthly-update-manually-add-pois'],
            external_task_id="end",
            external_task_ids=None,
            external_task_group_id=None,
            execution_date_fn=exec_delta_fn,
            check_existence=True,
            mode="reschedule",
            poke_interval=60,
        )

        start >> trigger_child_dag >> child_dag_sensor

        return group
