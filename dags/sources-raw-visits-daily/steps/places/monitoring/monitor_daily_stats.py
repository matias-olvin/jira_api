"""
DAG ID: poi_visits_pipeline_all_daily
"""
from datetime import datetime, timedelta

from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


def register(dag, start):
    """
    Register tasks on the dag

    Args:
        dag (airflow.models.DAG): DAG instance to register tasks on.
        start (airflow.models.TaskInstance): Task instance all tasks will
        be registered downstream from.
        mode (str): "update" or "backfill"

    Returns:
        airflow.models.TaskInstance: The last task node in this section.
    """

    delete_timeseries_analysis_mobility_identifiers_full = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="delete_time_series_analysis_mobility_identifiers_full",
        project_id=dag.params["project"],
        configuration={
            "query": {
                "query": "DELETE `{{ params['project'] }}.{{ params['metrics_dataset'] }}.{{ params['time_series_analysis_vis_block_groundtruth_table'] }}` where local_date = DATE_ADD( DATE('{{ ds }}'), INTERVAL -{{ var.value.latency_days_visits }} DAY )",
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
    timeseries_analysis_visits_mobility_identifiers_full = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="timeseries_analysis_visits_mobility_identifiers_full",
        project_id=dag.params["project"],
        configuration={
            "query": {
                "query": "{% include './bigquery/places/monitoring/timeseries_analysis_visits.sql' %}",
                "useLegacySql": "False",
                "destinationTable": {
                    "projectId": "{{ params['project'] }}",
                    "datasetId": "{{ params['metrics_dataset'] }}",
                    "tableId": "{{ params['time_series_analysis_vis_block_groundtruth_table'] }}",
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

    (
        start
        >> delete_timeseries_analysis_mobility_identifiers_full
        >> timeseries_analysis_visits_mobility_identifiers_full
    )

    return timeseries_analysis_visits_mobility_identifiers_full
