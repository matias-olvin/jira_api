"""
DAG ID: kpi_pipeline
"""
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.models import TaskInstance, DAG

def register(dag: DAG, start: TaskInstance, final):
    if final:
        bucket = dag.params["postgres_final_bucket"]
        dataset = dag.params["postgres_batch_dataset"]
        suffix = "_final"
        tables_to_send_to_gcs = dag.params["postgres_db_tables_in_bq"] + dag.params["data_feeds_tables"]
    else:
        bucket = dag.params["postgres_bucket"]
        dataset = dag.params["postgres_dataset"]
        suffix = ""

        all_postgres_tables_list = dag.params["postgres_db_tables_in_bq"]
        avoid_copy_from_postgres_to_postgres_batch_list = dag.params["avoid_copy_from_postgres_to_postgres_batch"]
        
        all_postgres_tables_filtered_list = list(filter(lambda x: x not in avoid_copy_from_postgres_to_postgres_batch_list, all_postgres_tables_list))

        tables_to_send_to_gcs = all_postgres_tables_filtered_list + dag.params["data_feeds_tables"]

    send_tables_end = DummyOperator(task_id=f"exportToGcs_{suffix}_end")

    def send_tables(table_bigquery: str, dataset_bigquery: str, start=start):
        if table_bigquery == "SGPlaceRaw":
            database_table = "SGPlace"
        else:
            database_table = table_bigquery

        # Clear monthly data
        clear_job = GCSDeleteObjectsOperator(
            gcp_conn_id="google_cloud_olvin_default",
            task_id=f"clearGCS_{database_table}{suffix}",
            depends_on_past=False,
            dag=dag,
            bucket_name=bucket,
            prefix=f"{database_table}/{{{{ dag_run.conf['update_year'] }}}}/{{{{ dag_run.conf['update_month'] }}}}/",
        )
        # Export to GCS
        export_job = BigQueryInsertJobOperator(
            gcp_conn_id="google_cloud_olvin_default",
            task_id=f"exportToGcs_{database_table}{suffix}",
            configuration={
                "extract": {
                    "destinationUris": (
                        (
                            f"gs://{bucket}/{database_table}/{{{{ dag_run.conf['update_year'] }}}}/{{{{ dag_run.conf['update_month'] }}}}/*.csv.gz"
                        )
                    ),
                    "printHeader": True,
                    "destinationFormat": "CSV",
                    "fieldDelimiter": "\t",
                    "compression": "GZIP",
                    "sourceTable": {
                        "projectId": "{{ params['project'] }}",
                        "datasetId": dataset_bigquery,
                        "tableId": table_bigquery,
                    },
                },
                "labels": {
                    "pipeline": "{{ dag.dag_id }}",
                    "task_id": "{{ task.task_id.lower()[:63] }}",
                },
            },
            dag=dag,
        )
        start >> clear_job >> export_job >> send_tables_end

    # send tables
    for table in tables_to_send_to_gcs:
        send_tables(table_bigquery=f"{table}", dataset_bigquery=dataset)

    return send_tables_end
