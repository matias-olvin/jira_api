"""
DAG ID: demographics_pipeline
"""
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
    create_device_homes = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="create_device_homes",
        configuration={
            "query": {
                "query": """CREATE TABLE IF NOT EXISTS
                `{{ params['project'] }}.{{ params['device_homes_dataset'] }}.{{ execution_date.add(months=1).strftime('%Y') }}`
                LIKE
                `{{ params['project'] }}.{{ params['device_homes_dataset'] }}.{{ execution_date.add(months=1).subtract(years=1).strftime('%Y') }}`""",
                "useLegacySql": "False",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        on_failure_callback=None,
        dag=dag,
        location="EU",
    )
    delete_device_homes = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="delete_device_homes",
        configuration={
            "query": {
                "query": """DELETE
                `{{ params['project'] }}.{{ params['device_homes_dataset'] }}.{{ execution_date.add(months=1).strftime('%Y') }}` \
                    where local_date >= '{{ ds.format('%Y-%m-01') }}'""",
                "useLegacySql": "False",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        on_failure_callback=None,
        dag=dag,
        location="EU",
    )

    query_device_homes = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="query_device_homes",
        configuration={
            "query": {
                "query": "{% include './bigquery/home_finder.sql' %}",
                "useLegacySql": "False",
                "destinationTable": {
                    "projectId": "{{ params['project'] }}",
                    "datasetId": "{{ params['device_homes_dataset'] }}",
                    "tableId": "{{ execution_date.add(months=1).strftime('%Y') }}",
                },
                "writeDisposition": "WRITE_APPEND",
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
    start >> create_device_homes >> delete_device_homes >> query_device_homes

    return query_device_homes
