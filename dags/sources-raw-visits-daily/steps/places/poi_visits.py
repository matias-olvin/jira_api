"""
DAG ID: visits_pipeline_all
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

    query_poi_visits = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="query_poi_visits",
        project_id=dag.params["project"],
        configuration={
            "query": {
                "query": "{% include './bigquery/places/poi_visits.sql' %}",
                "useLegacySql": "False",
                "destinationTable": {
                    "projectId": "{{ params['project'] }}",
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
    delete_poi_visits = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="delete_poi_visits",
        project_id=dag.params["project"],
        configuration={
            "query": {
                "query": "{% include './bigquery/places/delete_poi_visits.sql' %}",
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

    start >> delete_poi_visits >> query_poi_visits

    return query_poi_visits
