from airflow.models import DAG, TaskInstance
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup

from datetime import datetime, timedelta


def register(
    start: TaskInstance,
    dag: DAG,
    external_dag_id: str,
    use_postgres_batch_dataset: bool,
) -> TaskGroup:

    if use_postgres_batch_dataset:
        dataset = "{{ params['postgres_batch_dataset'] }}"
        execution_date = "{{ ds }}T00:00:01+00:00"
        id_label = "final"
    else:
        dataset = "{{ params['postgres_dataset'] }}"
        execution_date = "{{ ds }}"
        execution_delta = None
        id_label = "raw"

    def exec_delta_fn(execution_date):
        """
        > It takes the execution date and returns the execution date minus the difference between the
        execution date and the execution date with the time set to 00:00:00

        :param execution_date: The execution date of the task instance
        :return: The execution date minus the difference in seconds between the source and target
        execution dates.
        """
        source_exec_date = execution_date.replace(tzinfo=None)

        print(source_exec_date)

        if use_postgres_batch_dataset:
            target_exec_date = source_exec_date.strftime('%Y-%m-%dT00:00:01')
            target_exec_date = datetime.strptime(f"{target_exec_date}",'%Y-%m-%dT%H:%M:%S')
        else:
            target_exec_date = source_exec_date.strftime('%Y-%m-%dT00:00:00')
            target_exec_date = datetime.strptime(f"{target_exec_date}",'%Y-%m-%dT%H:%M:%S')

        diff = source_exec_date - target_exec_date
        diff_seconds = diff.total_seconds()

        return execution_date - timedelta(seconds=diff_seconds)

    with TaskGroup(group_id=f"visits_to_malls_{id_label}_trigger") as group:

        trigger_child_dag = TriggerDagRunOperator(
            task_id=f"trigger_{external_dag_id}_{id_label}",
            trigger_dag_id=external_dag_id,
            conf={"dataset_postgres_template": dataset},
            execution_date=execution_date,
            reset_dag_run=True,
        )

        child_dag_sensor = ExternalTaskSensor(
            task_id=f"{external_dag_id}_{id_label}_sensor",
            external_dag_id=external_dag_id,
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
