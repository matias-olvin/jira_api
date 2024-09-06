import pendulum
from airflow.models import DAG, TaskInstance, Variable
from airflow.operators.python import PythonOperator


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

    def set_airflow_var(key, val, lag=0):
        from time import sleep

        Variable.set(f"{key}", f"{val}")

        if lag > 0:
            sleep(lag)

    set_date = PythonOperator(
        task_id="set_current_date",
        python_callable=set_airflow_var,
        op_kwargs={
            "key": "smc_end_date",
            "val": (pendulum.now().subtract(days=1).format("YYYY-MM-DD")),
            "lag": 300,  # delay 5 minutes.
        },
        dag=dag,
    )
    start >> set_date

    return set_date
