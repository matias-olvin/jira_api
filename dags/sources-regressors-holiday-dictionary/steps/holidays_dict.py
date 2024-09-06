from datetime import datetime

from airflow.models import DAG, TaskInstance
from airflow.operators.bash import BashOperator
from common.operators.bigquery import OlvinBigQueryOperator
from common.processes.triggers import wait_for_sns_dag_run_completion


def register(start: TaskInstance, dag: DAG) -> TaskInstance:
    """Register tasks on the dag.

    Args:
        start (airflow.models.TaskInstance): Task instance all tasks will
            registered downstream from.
        dag (airflow.models.DAG): DAG instance to register tasks on.

    Returns:
        airflow.models.TaskInstance: The last task node in this section.
    """

    trigger_regressors_events_dictionary = BashOperator(
        task_id="trigger_regressors_events_dictionary",
        bash_command=f"gcloud composer environments run prod-sensormatic "
        f"--project {dag.params['sns_project']} "
        "--location europe-west1 "
        f"--impersonate-service-account {dag.params['cross_project_service_account']} "
        f'dags trigger -- -e "{{{{ ds }}}} {datetime.now().time()}" {dag.params["dag-time_factors_holidays_dictionary"]} '
        "|| true ",  # this line is needed due to gcloud bug.
        dag=dag,
    )

    wait_for_time_factors_holidays_dictionary = wait_for_sns_dag_run_completion(
        dag.params["dag-time_factors_holidays_dictionary"]
    )

    copy_task = OlvinBigQueryOperator(
        task_id=f"copy_holidays",
        query="{% include './include/bigquery/copy_holidays.sql' %}",
    )

    (
        start
        >> trigger_regressors_events_dictionary
        >> wait_for_time_factors_holidays_dictionary
        >> copy_task
    )

    return copy_task