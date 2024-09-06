"""
DAG ID: scaling_models_creation_trigger
"""
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

    def set_airflow_var(key, val):
        Variable.set(f"{key}", f"{val}")

    set_date = PythonOperator(
        task_id="set_execution_date",
        python_callable=set_airflow_var,
        op_kwargs={"key": "smc_demand_start_date", "val": "{{ ds }}"},
        dag=dag,
    )

    set_dummy_date = PythonOperator(
        task_id="set_end_date_dummy",
        python_callable=set_airflow_var,
        op_kwargs={"key": "smc_demand_end_date", "val": "{{ ds }}"},
        dag=dag,
    )

    start >> set_date >> set_dummy_date

    return set_dummy_date
