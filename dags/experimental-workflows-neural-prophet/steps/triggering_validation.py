"""
DAG ID: visits_estimation_model_development
"""
from airflow import AirflowException
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from common.operators.validations import TrendValidationOperator


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

    send_model_output_data_to_sns = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="send_model_output_data_to_sns",
        configuration={
            "query": {
                "query": "{% include './bigquery/triggering_model/move_tables_to_staging.sql' %}",
                "useLegacySql": "False",
            }
        },
        dag=dag,
    )

    #
    trigger_validation = TrendValidationOperator(
        task_id=f"reference_{dag.params['step']}_trend",
        sub_validation="group",
        env="dev",
        pipeline="visits_estimation_model_development",
        step=f"reference_{dag.params['step']}",
        run_date="2023-02-27",
        source=f"storage-dev-olvin-com.accessible_by_sns.visits_estimation_model_dev_{dag.params['step']}",
        destination="sns-vendor-olvin-poc.accessible_by_olvin_dev.visits_estimation_model_dev_metrics_trend",
        granularity="day",
        date_ranges=[["2022-04-01", "2022-06-30"], ["2021-01-01", "2021-12-31"]],
        classes=["fk_sgbrands", "naics_code", "region", "top_category"],
    )

    copy_validation_from_sns = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="copy_validation_from_sns",
        configuration={
            "query": {
                "query": "{% include './bigquery/model_output/copy_validation_from_sns.sql' %}",
                "useLegacySql": "False",
            }
        },
        dag=dag,
    )

    mark_success_to_run_post_triggering_validation = PythonOperator(
        task_id="mark_success_to_run_post_triggering_validation",
        python_callable=task_to_fail,
    )

    drop_data_sent_to_sns = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="drop_data_sent_to_sns",
        configuration={
            "query": {
                "query": "{% include './bigquery/triggering_model/drop_staging_tables.sql' %}",
                "useLegacySql": "False",
            }
        },
        dag=dag,
    )

    (
        start
        >> send_model_output_data_to_sns
        >> trigger_validation
        >> copy_validation_from_sns
        >> mark_success_to_run_post_triggering_validation
        >> drop_data_sent_to_sns
    )

    return drop_data_sent_to_sns
