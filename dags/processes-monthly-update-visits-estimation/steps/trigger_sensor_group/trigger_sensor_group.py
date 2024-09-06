from typing import Dict

from airflow.models import DAG, TaskInstance
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta


def register(
    start: TaskInstance,
    dag: DAG,
    trigger_dag_id: str,
    poke_interval: int = 60,
    conf: Dict = None,
) -> TaskGroup:

    def exec_delta_fn(execution_date):
        """
        > It takes the execution date and returns the execution date minus the difference between the
        execution date and the execution date with the time set to 00:00:01

        :param execution_date: The execution date of the task instance
        :return: The execution date minus the difference in seconds between the source and target
        execution dates.
        """
        source_exec_date = execution_date.replace(tzinfo=None)

        target_exec_date = source_exec_date.strftime('%Y-%m-%dT00:00:01')
        target_exec_date = datetime.strptime(f"{target_exec_date}",'%Y-%m-%dT%H:%M:%S')

        diff = source_exec_date - target_exec_date
        diff_seconds = diff.total_seconds()

        return execution_date - timedelta(seconds=diff_seconds)

    with TaskGroup(group_id=f"trigger_{trigger_dag_id}_task_group") as group:
        
        if conf is None:
            conf = {}
        
        trigger_dag_run = TriggerDagRunOperator(
            task_id=f"trigger_{trigger_dag_id}",
            trigger_dag_id=f"{{{{ params['{trigger_dag_id}'] }}}}",  # id of the child DAG
            conf=conf,  # Use to pass variables to triggered DAG.
            execution_date="{{ ds }}T00:00:01+00:00",
            reset_dag_run=True,
        )
        dag_run_sensor = ExternalTaskSensor(
            task_id=f"{trigger_dag_id}_sensor",
            external_dag_id=f"{{{{ params['{trigger_dag_id}'] }}}}",  # same as above
            external_task_id="end",
            external_task_ids=None,  # Use to wait for specifc tasks.
            external_task_group_id=None,  # Use to wait for specifc task group.
            execution_date_fn=exec_delta_fn,
            check_existence=True,  # Check DAG exists.
            mode="reschedule",
            poke_interval=poke_interval,  # choose appropriate time to poke, if dag runs for 10 minutes, poke every 5 for exmaple
        )

        start >> trigger_dag_run
        trigger_dag_run >> dag_run_sensor

    return group
