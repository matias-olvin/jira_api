"""
DAG ID: visits_estimation_model_development
"""
from airflow import AirflowException
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

    train = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="train",
        configuration={
            "query": {
                "query": "{% include './bigquery/model/train.sql' %}",
                "useLegacySql": "False",
            }
        },
        dag=dag,
    )

    output_validation = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="output_validation",
        configuration={
            "query": {
                "query": "{% include './bigquery/model/output_validation.sql' %}",
                "useLegacySql": "False",
            }
        },
        dag=dag,
    )

    start >> train >> output_validation

    mark_success_to_run_post_model_training = PythonOperator(
        task_id="mark_success_to_run_post_model_training",
        python_callable=task_to_fail,
    )

    output_validation >> mark_success_to_run_post_model_training

    return mark_success_to_run_post_model_training
