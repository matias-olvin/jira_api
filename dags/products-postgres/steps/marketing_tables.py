"""
DAG ID: kpi_pipeline
"""
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator


def register(dag, start):

    send_marketing_end = DummyOperator(task_id=f"send_marketing_end")

    def send_tables(table_bigquery: str, start=start):

        create_marketing_snapshot_task = BigQueryInsertJobOperator(
            gcp_conn_id="google_cloud_olvin_default",
            task_id=f"create_marketing_snapshot_{table_bigquery}",
            configuration={
            "query": {
                "query": (
                            '{% with tbl="'
                            f"{table_bigquery}"
                            '"%}{% include "./bigquery/marketing_tables.sql" '
                            '%}{% endwith %} '
                        ),
                "useLegacySql": "False",
            }
        },
        dag=dag,
        location="EU",
        )
        start >> create_marketing_snapshot_task >> send_marketing_end

    # send tables
    for table in dag.params["marketing_tables"]:
        send_tables(table_bigquery=f"{table}")

    return send_marketing_end
