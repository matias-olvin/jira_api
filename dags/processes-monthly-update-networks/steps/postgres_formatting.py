"""
DAG ID: sg_networks_pipeline
"""
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


def register(dag, start):
    """Register tasks on the dag.
    Args:
        dag (airflow.models.DAG): DAG instance to register tasks on.
        start (airflow.models.TaskInstance): Task instance all tasks will
            registered downstream from.
    Returns:
        airflow.models.TaskInstance: The last task node in this section.
    """

    query_connections_all = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="query_connections_all",
        configuration={
            "query": {
                "query": "{% include './bigquery/postgres_formatting/connections_all.sql' %}",
                "useLegacySql": "False",
                "destinationTable": {
                    "projectId": "{{ params['project'] }}",  # Variable.get("storage_project_id"),
                    "datasetId": "{{ params['networks_staging_dataset'] }}",
                    "tableId": "{{ params['connections_all_table'] }}",
                },
                "timePartitioning": {"type": "DAY", "field": "local_date"},
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_IF_NEEDED",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )

    query_chain_connections_all = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="query_chain_connections_all",
        configuration={
            "query": {
                "query": "{% include './bigquery/postgres_formatting/chain_connections_all.sql' %}",
                "useLegacySql": "False",
                "destinationTable": {
                    "projectId": "{{ params['project'] }}",  # Variable.get("storage_project_id"),
                    "datasetId": "{{ params['networks_staging_dataset'] }}",
                    "tableId": "{{ params['chain_connections_all_table'] }}",
                },
                "timePartitioning": {"type": "DAY", "field": "local_date"},
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_IF_NEEDED",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )

    query_journeys_all = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="query_journeys_all",
        configuration={
            "query": {
                "query": "{% include './bigquery/postgres_formatting/journeys_all.sql' %}",
                "useLegacySql": "False",
                "destinationTable": {
                    "projectId": "{{ params['project'] }}",  # Variable.get("storage_project_id"),
                    "datasetId": "{{ params['networks_staging_dataset'] }}",
                    "tableId": "{{ params['journeys_all_table'] }}",
                },
                "timePartitioning": {"type": "DAY", "field": "local_date"},
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_IF_NEEDED",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )

    postgres_formatting_end = DummyOperator(task_id="postgres_formatting_end", dag=dag)

    (
        start
        >> [query_connections_all, query_chain_connections_all, query_journeys_all]
        >> postgres_formatting_end
    )

    return postgres_formatting_end
