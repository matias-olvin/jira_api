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
    sg_raw_monitoring_start = DummyOperator(
        task_id="sg_raw_monitoring_start",
        dag=dag,
    )
    sg_raw_metadata = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="sg_raw_metadata",
        configuration={
            "query": {
                "query": "{% include './bigquery/monitoring/sg_raw_metadata.sql' %}",
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
    sg_raw_metadata_US = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="sg_raw_metadata_US",
        configuration={
            "query": {
                "query": "{% include './bigquery/monitoring/sg_raw_metadata_US.sql' %}",
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
    sg_raw_monitoring_end = DummyOperator(
        task_id="sg_raw_monitoring_end",
        dag=dag,
    )
    (
        start
        >> sg_raw_monitoring_start
        >> [sg_raw_metadata, sg_raw_metadata_US]
        >> sg_raw_monitoring_end
    )

    return sg_raw_monitoring_end
