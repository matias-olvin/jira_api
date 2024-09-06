from airflow.models import TaskInstance, Variable
from airflow.operators.python import PythonOperator


def register(start: TaskInstance) -> TaskInstance:
    """
    Register tasks on the dag

    Args:
        start (airflow.models.TaskInstance): Task instance all tasks will
        be registered downstream from.
        dag (airflow.models.DAG): DAG instance to register tasks on.
    Returns:
        airflow.models.TaskInstance: The last task node in this section.
    """

    def set_airflow_var(key):
        smc_start_date = Variable.get("smc_start_date")
        Variable.set(f"{key}", f"{smc_start_date}")

    set_manually_add_pois_date = PythonOperator(
        task_id="set_manually_add_pois_date",
        python_callable=set_airflow_var,
        op_kwargs={"key": "manually_add_pois_deadline_date"},
    )

    start >> set_manually_add_pois_date

    return set_manually_add_pois_date
