"""
DAG ID: smc_visits_daily_backfill
"""
from airflow.models import Variable
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
    delete_poi_visits = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="delete_poi_visits",
        project_id=Variable.get("env_project"),
        configuration={
            "query": {
                "query": "DELETE `{{ var.value.env_project }}.{{ params['poi_visits_dataset'] }}.{{ execution_date.subtract(days=var.value.latency_days_visits|int).strftime('%Y') }}` WHERE local_date = DATE_SUB('{{ ds }}', INTERVAL {{ var.value.latency_days_visits|int }} DAY)",
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
    query_poi_visits = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="query_poi_visits",
        project_id=Variable.get("env_project"),
        configuration={
            "query": {
                "query": "{% include './bigquery/places/poi_visits.sql' %}",
                "useLegacySql": "False",
                "destinationTable": {
                    "projectId": "{{ var.value.env_project }}",
                    "datasetId": "{{ params['poi_visits_dataset'] }}",
                    "tableId": "{{ execution_date.subtract(days=var.value.latency_days_visits|int).strftime('%Y') }}"
                    + "${{ execution_date.subtract(days=var.value.latency_days_visits|int).strftime('%Y%m%d') }}",
                },
                "timePartitioning": {"type": "DAY", "field": "local_date"},
                "clustering": {
                    "fields": [
                        "lat_long_visit_point",
                        "country",
                        "fk_sgplaces",
                        "device_id",
                    ]
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
    start >> delete_poi_visits >> query_poi_visits

    return query_poi_visits
