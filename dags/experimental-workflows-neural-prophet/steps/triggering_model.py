"""
DAG ID: visits_estimation_model_development
"""
from datetime import datetime

from airflow import AirflowException
from airflow.operators.bash_operator import BashOperator
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

    #

    trigger_visits_estimation_model = BashOperator(
        task_id="visits_estimation_trigger",
        bash_command="gcloud composer environments run {{ params['sns_composer'] }} "
        "--project {{ params['sns_project'] }} "
        "--location {{ params['sns_composer_location'] }} "
        "--impersonate-service-account {{ params['cross_project_service_account'] }} "
        f'dags trigger -- {dag.params["visits_estimation_pipeline"]} -e "{{{{ ds }}}} {datetime.now().time()}" '
        "|| true ",  # this line is needed due to gcloud bug.
    )

    copy_visits_estimation_output_from_sns = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="copy_visits_estimation_output_from_sns",
        configuration={
            "query": {
                "query": "{% include './bigquery/model_output/copy_visits_estimation_output_from_sns.sql' %}",
                "useLegacySql": "False",
            }
        },
        dag=dag,
    )

    mark_success_to_run_post_triggering_model = PythonOperator(
        task_id="mark_success_to_run_post_triggering_model",
        python_callable=task_to_fail,
    )

    (
        start
        >> send_model_input_data_to_sns
        >> trigger_visits_estimation_model
        >> copy_visits_estimation_output_from_sns
        >> mark_success_to_run_post_triggering_model
    )

    return mark_success_to_run_post_triggering_model
