from airflow.models import DAG, TaskInstance
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


def register(start: TaskInstance, dag: DAG) -> TaskInstance:
    """Register tasks on the dag.

    Args:
        start (airflow.models.TaskInstance): Task instance all tasks will
            registered downstream from.
        dag (airflow.models.DAG): The DAG object to register tasks on.

    Returns:
        airflow.models.TaskInstance: The last task node in this section.
    """

    create_device_zips_v2 = BigQueryInsertJobOperator(
        gcp_conn_id="google_cloud_olvin_default",
        task_id="create_device_zips_v2",
        configuration={
            "query": {
                "query": """CREATE TABLE IF NOT EXISTS
                `{{ params['project'] }}.{{ params['device_zipcodes_v2_dataset'] }}.{{ execution_date.add(months=1).strftime('%Y') }}`
                LIKE
                `{{ params['project'] }}.{{ params['device_zipcodes_v2_dataset'] }}.{{ execution_date.add(months=1).subtract(years=1).strftime('%Y') }}`""",
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
    delete_device_zips_v2 = BigQueryInsertJobOperator(
        gcp_conn_id="google_cloud_olvin_default",
        task_id="delete_device_zips_v2",
        configuration={
            "query": {
                "query": """DELETE
                `{{ params['project'] }}.{{ params['device_zipcodes_v2_dataset'] }}.{{ execution_date.add(months=1).strftime('%Y') }}` \
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
    query_device_zips_v2 = BigQueryInsertJobOperator(
        gcp_conn_id="google_cloud_olvin_default",
        task_id="query_device_zips_v2",
        configuration={
            "query": {
                "query": "{% include './bigquery/zip_finder_v2.sql' %}",
                "useLegacySql": "False",
                "destinationTable": {
                    "projectId": "{{ params['project'] }}",
                    "datasetId": "{{ params['device_zipcodes_v2_dataset'] }}",
                    "tableId": "{{ execution_date.strftime('%Y') }}",
                    # "tableId": "{{ execution_date.add(months=1).strftime('%Y') }}",
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
    start >> create_device_zips_v2 >> delete_device_zips_v2 >> query_device_zips_v2

    return query_device_zips_v2
