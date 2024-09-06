"""
DAG ID: visits_pipeline
"""
import os
from datetime import datetime
from importlib.machinery import SourceFileLoader

from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

file_path = os.path.dirname(os.path.realpath(__file__))
trigger = SourceFileLoader(
    "trigger", f"{file_path.split('/postgres')[0]}/trigger.py"
).load_module()


def register(dag, start):
    """Register tasks on the dag.

    Args:
        dag (airflow.models.DAG): DAG instance to register tasks on.
        start (airflow.models.TaskInstance): Task instance all tasks will
            registered downstream from.

    Returns:
        airflow.models.TaskInstance: The last task node in this section.

    ------



    """

    SGPlaceDailyVisitsRaw = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="SGPlaceDailyVisitsRaw",
        project_id=Variable.get("env_project"),
        configuration={
            "query": {
                "query": "{% include './bigquery/postgres/SGPlaceDailyVisitsRaw.sql' %}",
                "useLegacySql": "False",
                "create_session": "True",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id |  replace('.','-') }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )

    SGPlaceHourlyAllVisitsRaw = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="SGPlaceHourlyAllVisitsRaw",
        project_id=Variable.get("env_project"),
        configuration={
            "query": {
                "query": "{% include './bigquery/postgres/SGPlaceHourlyAllVisitsRaw.sql' %}",
                "useLegacySql": "False",
                "create_session": "True",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id |  replace('.','-') }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )

    SGPlaceHourlyVisitsRaw = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="SGPlaceHourlyVisitsRaw",
        project_id=Variable.get("env_project"),
        configuration={
            "query": {
                "query": "{% include './bigquery/postgres/SGPlaceHourlyVisitsRaw.sql' %}",
                "useLegacySql": "False",
                "create_session": "True",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id |  replace('.','-') }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )

    SGPlaceMonthlyVisitsRaw = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="SGPlaceMonthlyVisitsRaw",
        project_id=Variable.get("env_project"),
        configuration={
            "query": {
                "query": "{% include './bigquery/postgres/SGPlaceMonthlyVisitsRaw.sql' %}",
                "useLegacySql": "False",
                "create_session": "True",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id |  replace('.','-') }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )

    SGPlaceRanking = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="SGPlaceRanking",
        project_id=Variable.get("env_project"),
        configuration={
            "query": {
                "query": "{% include './bigquery/postgres/SGPlaceRanking.sql' %}",
                "useLegacySql": "False",
                "create_session": "True",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id |  replace('.','-') }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )

    ZipCodeDailyVisitsRaw = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="ZipCodeDailyVisitsRaw",
        project_id=Variable.get("env_project"),
        configuration={
            "query": {
                "query": "{% include './bigquery/postgres/ZipCodeDailyVisitsRaw.sql' %}",
                "useLegacySql": "False",
                "create_session": "True",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id |  replace('.','-') }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )

    ZipCodeHourlyVisitsRaw = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="ZipCodeHourlyVisitsRaw",
        project_id=Variable.get("env_project"),
        configuration={
            "query": {
                "query": "{% include './bigquery/postgres/ZipCodeHourlyVisitsRaw.sql' %}",
                "useLegacySql": "False",
                "create_session": "True",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id |  replace('.','-') }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )

    SGPlacePatternVisitsRaw = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="SGPlacePatternVisitsRaw",
        project_id=Variable.get("env_project"),
        configuration={
            "query": {
                "query": "{% include './bigquery/postgres/SGPlacePatternVisitsRaw.sql' %}",
                "useLegacySql": "False",
                "create_session": "True",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id |  replace('.','-') }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )

    dummy_end_data_preparation = DummyOperator(
        task_id="dummy_end_data_preparation", dag=dag
    )

    start >> [SGPlaceDailyVisitsRaw, SGPlaceHourlyAllVisitsRaw]

    SGPlaceHourlyAllVisitsRaw >> [SGPlaceHourlyVisitsRaw, ZipCodeHourlyVisitsRaw]

    SGPlaceDailyVisitsRaw >> [SGPlaceMonthlyVisitsRaw, ZipCodeDailyVisitsRaw]

    SGPlaceMonthlyVisitsRaw >> SGPlaceRanking

    [SGPlaceDailyVisitsRaw, SGPlaceHourlyAllVisitsRaw] >> SGPlacePatternVisitsRaw

    [
        SGPlaceHourlyVisitsRaw,
        ZipCodeHourlyVisitsRaw,
        ZipCodeDailyVisitsRaw,
        SGPlaceRanking,
        SGPlacePatternVisitsRaw,
    ] >> dummy_end_data_preparation

    delete_time_series_zipcode_visualisations = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="delete_time_series_zipcode_visualisations",
        configuration={
            "query": {
                "query": f"""DELETE `{{{{ var.value.env_project }}}}.{{{{ params['metrics_dataset'] }}}}.{{{{ params['time_series_visualisations_zipcode_table'] }}}}` where run_date = '{{{{ ds }}}}' AND step = 'adjustments' AND mode = 'all'""",
                "useLegacySql": "False",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id |  replace('.','-') }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )

    time_series_zipcode_visualisations = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="time_series_zipcode_visualisations",
        configuration={
            "query": {
                "query": "{% include './bigquery/postgres/metrics/time_series_vis_zipcode.sql' %}",
                "useLegacySql": "False",
                "destinationTable": {
                    "projectId": "{{ var.value.env_project }}",
                    "datasetId": "{{ params['metrics_dataset'] }}",
                    "tableId": "{{ params['time_series_visualisations_zipcode_table'] }}",
                },
                "writeDisposition": "WRITE_APPEND",
                "createDisposition": "CREATE_IF_NEEDED",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id |  replace('.','-') }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )
    (
        dummy_end_data_preparation
        >> delete_time_series_zipcode_visualisations
        >> time_series_zipcode_visualisations
    )
    return time_series_zipcode_visualisations
