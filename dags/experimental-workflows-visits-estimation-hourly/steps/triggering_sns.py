"""
DAG ID: visits_estimation_model_development
"""
from airflow import AirflowException
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from common.processes.triggers import trigger_dag_run


def task_to_fail():
    """
    Task that will fail.
    """
    raise AirflowException("This task must be manually set to success to continue.")


def register(dag, start):
    """Register tasks on the dag.

    Args:
        dag (airflow.models.DAG): DAG instance to register tasks on.
        start (airflow.models.TaskInstance): Task instance all tasks will
            registered downstream from.

    Returns:
        airflow.models.TaskInstance: The last task node in this section.
    """

    send_model_input_data_to_sns = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="send_model_input_data_to_sns",
        configuration={
            "query": {
                "query": "{% include './bigquery/triggering_model/move_tables_to_staging.sql' %}",
                "useLegacySql": "False",
            }
        },
        dag=dag,
    )

    trigger_sns_for_hourly = trigger_dag_run(
        start=start,
        env="dev",
        trigger_dag_id="trigger_sns_for_hourly",
        destination_table=[
            f"{Variable.get('env_project')}.{dag.params['visits_estimation_dataset']}.adjustments_hourly_gt_visits",
        ],
    )

    mark_success_to_run_post_triggering_model = PythonOperator(
        task_id="mark_success_to_run_post_triggering_model",
        python_callable=task_to_fail,
    )

    (
        start
        >> send_model_input_data_to_sns
        >> trigger_sns_for_hourly
        >> mark_success_to_run_post_triggering_model
    )

    return mark_success_to_run_post_triggering_model
