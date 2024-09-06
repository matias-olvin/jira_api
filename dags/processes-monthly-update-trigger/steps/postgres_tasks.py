from datetime import datetime, timedelta

from airflow.models import DAG, TaskInstance, Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor


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

    def exec_delta_fn(execution_date, context):
        """
        > It takes the execution date and returns the execution date minus the difference between the
        execution date and the execution date with the time set to 00:00:00

        :param execution_date: The execution date of the task instance
        :return: The execution date minus the difference in seconds between the source and target
        execution dates.
        """
        ti = context["ti"]
        source_exec_date = execution_date.replace(tzinfo=None)
        target_exec_date = datetime.strptime(
            f"{ti.xcom_pull(task_ids='push_execution_date')}T00:00:00",
            "%Y-%m-%dT00:00:00",
        )
        diff = source_exec_date - target_exec_date
        diff_seconds = diff.total_seconds()

        return execution_date - timedelta(seconds=diff_seconds)

    postgres_dag_id = dag.params["dag-products-postgres"]

    # Triggering the send_to_postgres_pipeline DAG.
    trigger_send_to_postgres = TriggerDagRunOperator(
        task_id=f"trigger_{postgres_dag_id}",
        trigger_dag_id=f"{postgres_dag_id}",
        execution_date="{{ task_instance.xcom_pull(task_ids='push_execution_date') }}",
        conf={
            "update_year": f"{int(Variable.get('monthly_update').split('-')[0])}",
            "update_month": f"{int(Variable.get('monthly_update').split('-')[1])}",
        },
    )
    # Waiting for the send_to_postgres_pipeline to finish before moving on.
    wait_for_send_to_postgres = ExternalTaskSensor(
        task_id=f"wait_for_{postgres_dag_id}",
        external_dag_id=f"{postgres_dag_id}",
        execution_date_fn=exec_delta_fn,
        poke_interval=60 * 60 * 2,
        mode="reschedule",
    )
    # Chaining the tasks together.
    start >> trigger_send_to_postgres >> wait_for_send_to_postgres

    return wait_for_send_to_postgres
