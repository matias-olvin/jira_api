"""
DAG ID: monitor_pipeline
"""
# from google.cloud import bigquery
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryValueCheckOperator,
)


def register(dag, start, mode="update"):
    monitor_poi_visits_scaled = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="monitor_poi_visits_scaled",
        project_id=dag.params["project"],
        configuration={
            "query": {
                "query": "{% include './bigquery/tests/poi_visits_scaled.sql' %}",
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

    monitor_check_poi_visits_scaled = BigQueryValueCheckOperator(
gcp_conn_id="google_cloud_olvin_default",
        project_id=dag.params["project"],
        task_id="monitor_check_poi_visits_scaled",
        retries=1,
        dataset_id="{{ params['monitoring_dataset'] }}",
        table_id="{{ params['poi_scaled_data_dataset'] }}",
        sql=(
            f"SELECT terminate_ FROM `"
            "{{ params['project'] }}"
            ".{{ dag.params['monitoring_dataset'] }}.{{dag.params['poi_visits_scaled_met_areas_dataset']}}` WHERE \
            local_date = "
            "'{{ execution_date.subtract(days=var.value.latency_days_visits|int).format('%Y-%m-%d') }}'"
        ),
        use_legacy_sql=False,
        pass_value=False,
        labels={
            "pipeline": "{{ dag.dag_id }}",
            "task_id": "{{ task.task_id.lower()[:63] }}",
        },
        location="EU",
        dag=dag,
    )

    start >> monitor_poi_visits_scaled >> monitor_check_poi_visits_scaled

    return monitor_check_poi_visits_scaled
