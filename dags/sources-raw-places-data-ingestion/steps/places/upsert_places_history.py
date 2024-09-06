"""
DAG ID: dynamic_places
"""
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
    upsert_places_history = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="upsert_places_history",
        configuration={
            "query": {
                "query": "{% include './bigquery/places/upsert_places_history.sql' %}",
                "useLegacySql": "False",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )
    (start >> upsert_places_history)
    return upsert_places_history
