"""
DAG ID: visits_estimation_model_development
"""
from airflow import AirflowException
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


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

    start_hourly = DummyOperator(task_id="start_adjustments_hourly_input", dag=dag)
    start >> start_hourly

    prepare_input_data = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="prepare_input_data",
        configuration={
            "query": {
                "query": "{% include './bigquery/input_preparation/prepare_input_data.sql' %}",
                "useLegacySql": "False",
            }
        },
        dag=dag,
    )

    start_hourly >> prepare_input_data

    mark_success_to_run_post_input_preparation = PythonOperator(
        task_id="mark_success_to_run_post_input_preparation",
        python_callable=task_to_fail,
    )

    prepare_input_data >> mark_success_to_run_post_input_preparation

    # return send_data_to_sns
    return mark_success_to_run_post_input_preparation
