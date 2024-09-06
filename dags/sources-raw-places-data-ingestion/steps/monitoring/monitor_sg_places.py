"""
DAG ID: dynamic_places
"""
from airflow.operators.dummy_operator import DummyOperator
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
    monitoring_start = DummyOperator(
        task_id="sg_places_monitoring_start",
        dag=dag,
    )
    update_sg_places_metadata = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="update_sg_places_metadata",
        configuration={
            "query": {
                "query": "{% include './bigquery/monitoring/places_metadata.sql' %}",
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
    update_sg_places_placekey_stats = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="update_sg_places_placekey_stats",
        configuration={
            "query": {
                "query": "{% include './bigquery/monitoring/places_placekey_stats.sql' %}",
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
    monitoring_end = DummyOperator(
        task_id="sg_places_monitoring_end",
        dag=dag,
    )
    (
        start
        >> monitoring_start
        >> [update_sg_places_metadata, update_sg_places_placekey_stats]
        >> monitoring_end
    )
    return monitoring_end
