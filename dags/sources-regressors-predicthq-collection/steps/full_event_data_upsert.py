"""
DAG ID: predicthq_events_collection
"""
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


def register(dag, start):
    """
    Register tasks on the dag

    Args:
        dag (airflow.models.DAG): DAG instance to register tasks on.
        start (airflow.models.TaskInstance): Task instance all tasks will
        be registered downstream from.

    Returns:
        airflow.models.TaskInstance: The last task node in this section.
    """
    full_event_data_upsert = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="full_event_data_upsert",
        configuration={
            "query": {
                "query": "{% include './bigquery/full_event_data_upsert.sql' %}",
                "useLegacySql": "False",
            },
            "labels": {
                "dag_id": "{{ dag.dag_id }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )
    (start >> full_event_data_upsert)
    return full_event_data_upsert
