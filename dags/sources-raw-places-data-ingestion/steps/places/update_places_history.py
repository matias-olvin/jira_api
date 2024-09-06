"""
DAG ID: dynamic_places
"""
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from common.operators.bigquery import OlvinBigQueryOperator


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
    update_places_history_sg_id_active = BigQueryInsertJobOperator(
        gcp_conn_id="google_cloud_olvin_default",
        task_id="update_places_history_sg_id_active",
        configuration={
            "query": {
                "query": "{% include './bigquery/places/update_places_history_sg_id_active.sql' %}",
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
    update_places_history_olvin_id = BigQueryInsertJobOperator(
        gcp_conn_id="google_cloud_olvin_default",
        task_id="update_places_history_olvin_id",
        configuration={
            "query": {
                "query": "{% include './bigquery/places/update_places_history_olvin_id.sql' %}",
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
    update_places_history_olvin_id_nulls = BigQueryInsertJobOperator(
        gcp_conn_id="google_cloud_olvin_default",
        task_id="update_places_history_olvin_id_nulls",
        configuration={
            "query": {
                "query": "{% include './bigquery/places/update_places_history_olvin_id_nulls.sql' %}",
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
    update_places_history_site_id = BigQueryInsertJobOperator(
        gcp_conn_id="google_cloud_olvin_default",
        task_id="update_places_history_site_id",
        configuration={
            "query": {
                "query": "{% include './bigquery/places/update_places_history_site_id.sql' %}",
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
    update_places_history_fk_parents_olvin = BigQueryInsertJobOperator(
        gcp_conn_id="google_cloud_olvin_default",
        task_id="update_places_history_fk_parents_olvin",
        configuration={
            "query": {
                "query": "{% include './bigquery/places/update_places_history_fk_parents_olvin.sql' %}",
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
    deactivate_duplicates_places_history = BigQueryInsertJobOperator(
        gcp_conn_id="google_cloud_olvin_default",
        task_id="deactivate_duplicates_places_history",
        configuration={
            "query": {
                "query": "{% include './bigquery/places/deactivate_duplicates_places_history.sql' %}",
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

    deactivate_duplicate_actives_places_history = OlvinBigQueryOperator(
        task_id="deactivate_duplicate_actives_places_history",
        query="{% include './bigquery/places/deactivate_duplicate_actives_places_history.sql' %}",
    )

    clear_bucket_places_history = GCSDeleteObjectsOperator(
        task_id="clear_bucket_places_history",
        depends_on_past=False,
        dag=dag,
        bucket_name=dag.params["history_backup_bucket"],
        prefix="{{ ds.split('-')[0] }}/{{ ds.split('-')[1] }}/places/",
    )
    export_places_history = BigQueryInsertJobOperator(
        gcp_conn_id="google_cloud_olvin_default",
        task_id="export_places_history",
        configuration={
            "extract": {
                "destinationUri": "gs://{{ params['history_backup_bucket'] }}/{{ ds.split('-')[0] }}/{{ ds.split('-')[1] }}/places/*.csv.gzip",
                "printHeader": True,
                "destinationFormat": "CSV",
                "compression": "GZIP",
                "sourceTable": {
                    "projectId": "{{ var.value.env_project }}",
                    "datasetId": "{{ params['staging_data_dataset'] }}",
                    "tableId": "{{ params['all_places_table'] }}",
                },
            }
        },
    )
    (
        start
        >> update_places_history_sg_id_active
        >> update_places_history_olvin_id
        >> update_places_history_olvin_id_nulls
        >> update_places_history_fk_parents_olvin
        >> update_places_history_site_id
        >> deactivate_duplicates_places_history
        >> deactivate_duplicate_actives_places_history
        >> clear_bucket_places_history
        >> export_places_history
    )

    return export_places_history
