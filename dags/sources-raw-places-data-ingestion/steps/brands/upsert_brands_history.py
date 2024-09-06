"""
DAG ID: dynamic_places
"""
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator


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
    upsert_places_to_brands_history = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="upsert_places_to_brands_history",
        configuration={
            "query": {
                "query": "{% include './bigquery/brands/upsert_places_to_brands_history.sql' %}",
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
    upsert_brands_to_brands_history = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="upsert_brands_to_brands_history",
        configuration={
            "query": {
                "query": "{% include './bigquery/brands/upsert_brands_to_brands_history.sql' %}",
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
    drop_brands_history_duplicates = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="drop_brands_history_duplicates",
        configuration={
            "query": {
                "query": "{% include './bigquery/brands/drop_brands_history_duplicates.sql' %}",
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
    ammend_brands_history_activity = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="ammend_brands_history_activity",
        configuration={
            "query": {
                "query": "{% include './bigquery/brands/ammend_brands_history_activity.sql' %}",
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
    clear_bucket_brands_history = GCSDeleteObjectsOperator(
        task_id="clear_bucket_brands_history",
        depends_on_past=False,
        dag=dag,
        bucket_name=dag.params["history_backup_bucket"],
        prefix="{{ ds.split('-')[0] }}/{{ ds.split('-')[1] }}/brands/",
    )
    export_brands_history = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="export_brands_history",
        configuration={
            "extract": {
                "destinationUri": "gs://{{ params['history_backup_bucket'] }}/{{ ds.split('-')[0] }}/{{ ds.split('-')[1] }}/brands/*.csv.gzip",
                "printHeader": True,
                "destinationFormat": "CSV",
                "compression": "GZIP",
                "sourceTable": {
                    "projectId": "{{ var.value.env_project }}",
                    "datasetId": "{{ params['staging_data_dataset'] }}",
                    "tableId": "{{ params['all_brands_table'] }}",
                },
            }
        },
    )
    (
        start
        >> upsert_places_to_brands_history
        >> upsert_brands_to_brands_history
        >> drop_brands_history_duplicates
        >> ammend_brands_history_activity
        >> clear_bucket_brands_history
        >> export_brands_history
    )

    return export_brands_history
