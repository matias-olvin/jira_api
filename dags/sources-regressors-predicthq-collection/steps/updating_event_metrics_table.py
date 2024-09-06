"""
DAG ID: predicthq_events_collection
"""
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


def register(dag, start):
    updating_event_metrics_table = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="updating_event_metrics_table",
        configuration={
            "query": {
                "query": "{% include './bigquery/updating_event_metrics_table.sql' %}",
                "useLegacySql": "False",
                "destinationTable": {
                    "projectId": "{{ var.value.env_project }}",
                    "datasetId": "{{ params['regressors_metrics_dataset'] }}",
                    "tableId": "{{ params['predicthq_events_agg_table']}}",
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

    start >> updating_event_metrics_table

    return updating_event_metrics_table
