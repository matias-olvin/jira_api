"""
DAG ID: predicthq_events_collection
"""
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


def register(dag, start):
    preprocessing_update_data = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="preprocessing_update_data",
        configuration={
            "query": {
                "query": "{% include './bigquery/preprocessing_update_data.sql' %}",
                "useLegacySql": "False",
                "destinationTable": {
                    "projectId": "{{ var.value.env_project }}",
                    "datasetId": "{{ params['regressors_staging_dataset'] }}",
                    "tableId": "{{ params['preprocessed_daily_data_table']}}",
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_IF_NEEDED",
            },
            "labels": {
                "dag_id": "{{ dag.dag_id }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )

    start >> preprocessing_update_data

    return preprocessing_update_data
