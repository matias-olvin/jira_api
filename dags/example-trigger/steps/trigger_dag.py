from airflow.models import DAG, TaskInstance
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup


def register(start: TaskInstance, dag: DAG) -> TaskGroup:
    with TaskGroup(group_id="dag_trigger") as group:
        trigger_dag_run = TriggerDagRunOperator(
            task_id="trigger_child_dag",
            trigger_dag_id="example-trigger-child",
            conf={},  # Use to pass variables to triggered DAG.
            execution_date="{{ ds }}",
            reset_dag_run=True,
        )
        dag_run_sensor = ExternalTaskSensor(
            task_id="child_dag_sensor",
            external_dag_id="example-trigger-child",
            external_task_id="end",  # Use to wait for specifc task.
            external_task_ids=None,  # Use to wait for specifc tasks.
            external_task_group_id=None,  # Use to wait for specifc task group.
            check_existence=True,  # Check DAG exists.
            # mode="reschedule",
            # poke_interval=60,
        )

        start >> trigger_dag_run
        trigger_dag_run >> dag_run_sensor

    return group
