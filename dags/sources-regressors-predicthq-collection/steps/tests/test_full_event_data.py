"""
DAG ID: predicthq_events_collection
"""
from common.utils import callbacks
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryValueCheckOperator,
)


def register(dag, start):
    """Register tasks on the dag.

    Args:
        dag (airflow.models.DAG): DAG instance to register tasks on.
        start (airflow.models.TaskInstance): Task instance all tasks will
            registered downstream from.

    Returns:
        airflow.models.TaskInstance: The last task node in this section.
    """
    test_full_event_data = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="test_full_event_data",
        configuration={
            "query": {
                "query": "{% include './bigquery/tests/test_full_event_data.sql' %}",
                "useLegacySql": False,
            },
            "labels": {
                "dag_id": "{{ dag.dag_id }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
    )
    check_full_event_data = BigQueryValueCheckOperator(
gcp_conn_id="google_cloud_olvin_default",
        task_id="check_full_event_data",
        sql="""
            SELECT
                result
            FROM
                `{{ var.value.env_project }}.{{ params['test_compilation_dataset'] }}.{{ params['test_results_table'] }}`
            WHERE
                regressor = 'regressors_collection_predicthq_events' AND
                test_name = 'daily data upsert' AND
                run_date = '{{ ds }}'
        """,
        pass_value=True,
        dag=dag,
        location="EU",
        use_legacy_sql=False,
        retries=0,
        on_failure_callback=callbacks.task_fail_slack_alert(
            "U034YDXAD1R",  # Jake
        ),
    )
    (start >> test_full_event_data >> check_full_event_data)

    return check_full_event_data
