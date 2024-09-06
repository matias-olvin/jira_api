from datetime import datetime, timedelta

from airflow.models import DAG, TaskInstance
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
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
        target_exec_date = datetime.strptime(f"{target_exec_date}", "%Y-%m-%dT%H:%M:%S")

        diff = source_exec_date - target_exec_date
        diff_seconds = diff.total_seconds()

        return execution_date - timedelta(seconds=diff_seconds)

    delay = BashOperator(task_id="delay", dag=dag, bash_command="sleep 10")

    trigger_regressors_group_visits_collection = BashOperator(
        task_id="trigger_regressors_group_visits_collection",
        bash_command=f"gcloud composer environments run prod-sensormatic "
        f"--project {dag.params['sns_project']} "
        "--location europe-west1 "
        f"--impersonate-service-account {dag.params['cross_project_service_account']} "
        f'dags trigger -- -e "{{{{ ds }}}} {datetime.now().time()}" {dag.params["dag-time_factors_group_residuals_collection"]} '
        "|| true ",  # this line is needed due to gcloud bug.
        dag=dag,
    )

    wait_for_regressors_group_visits_collection = wait_for_sns_dag_run_completion(
        dag.params["dag-time_factors_group_residuals_collection"],
        retry_delay=60 * 3,
    )

    trigger_group_residuals_dictionaries = TriggerDagRunOperator(
        task_id=f"trigger_{dag.params['dag-sources-regressors-group-visits-dictionary']}",
        trigger_dag_id=dag.params["dag-sources-regressors-group-visits-dictionary"],
        execution_date="{{ ds }}",
        dag=dag,
    )

    wait_for_pipeline_group_residuals_dictionaries = ExternalTaskSensor(
        task_id=f"wait_for_{dag.params['dag-sources-regressors-group-visits-dictionary']}",
        external_dag_id=dag.params["dag-sources-regressors-group-visits-dictionary"],
        execution_date_fn=exec_delta_fn,
        external_task_id="end",
        poke_interval=60 * 60 * 2,
        mode="reschedule",
        dag=dag,
    )

    copy_task = OlvinBigQueryOperator(
        task_id="copy_group_visits",
        query="{% include './include/bigquery/copy_group_visits.sql' %}",
    )

    (
        start
        >> delay
        >> trigger_regressors_group_visits_collection
        >> wait_for_regressors_group_visits_collection
        >> trigger_group_residuals_dictionaries
        >> wait_for_pipeline_group_residuals_dictionaries
        >> copy_task
    )

    return copy_task
