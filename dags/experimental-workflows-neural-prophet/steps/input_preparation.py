"""
DAG ID: visits_estimation_model_development
"""
from datetime import date

import pandas as pd
from airflow import AirflowException
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from dateutil.relativedelta import relativedelta


def task_to_fail():
    """
    Task that will fail.
    """
    raise AirflowException("This task must be manually set to success to continue.")


def register(dag, start):
    """Register tasks on the dag.

    Args:
        dag (airflow.models.DAG): DAG instance to register tasks on.
        start (airflow.models.TaskInstance): Task instance all tasks will
            registered downstream from.

    Returns:
        airflow.models.TaskInstance: The last task node in this section.
    """
    start_poi = DummyOperator(task_id="start_poi_inference_input", dag=dag)
    start >> start_poi
    years_compute = pd.date_range(
        "2019-01-01",
        (date.today() + relativedelta(days=150)).strftime("%Y-%m-%d"),
        freq="YS",
        inclusive="left",
    )

    for year_start in years_compute:
        date_start = year_start.strftime("%Y-%m-%d")
        year_name = year_start.strftime("%Y")
        date_end = (year_start + relativedelta(years=1)).strftime("%Y-%m-%d")
        query_input_poi = BigQueryInsertJobOperator(
            task_id=f"query_input_poi_{year_name}",
            configuration={
                "query": {
                    "query": f"{{% with "
                    f"date_start='{date_start}', "
                    f"date_end='{date_end}', "
                    f"year_name='{year_name}'"
                    f"%}}{{% include './bigquery/input_preparation/prepare_input.sql' %}}"
                    f"{{% endwith %}}",
                    "useLegacySql": "False",
                    "createSession": "True",
                },
                "labels": {
                    "pipeline": "{{ dag.dag_id }}",
                    "task_id": "{{ task.task_id.lower()[:63] }}",
                },
            },
            dag=dag,
            location="EU",
        )
        start_poi >> query_input_poi
        start_poi = query_input_poi

    input_merge = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="input_merge",
        configuration={
            "query": {
                "query": "{% include './bigquery/input_preparation/merge.sql' %}",
                "useLegacySql": "False",
            }
        },
        dag=dag,
    )

    event_input_task = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="event_input_task",
        configuration={
            "query": {
                "query": "{% include './bigquery/input_preparation/prepare_events.sql' %}",
                "useLegacySql": "False",
            }
        },
        dag=dag,
    )

    start >> event_input_task

    query_grouping_id = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="grouping_id",
        configuration={
            "query": {
                "query": "{% include './bigquery/input_preparation/grouping_id.sql' %}",
                "useLegacySql": "False",
            }
        },
        dag=dag,
    )

    start >> query_grouping_id

    mark_success_to_run_post_input_preparation = PythonOperator(
        task_id="mark_success_to_run_post_input_preparation",
        python_callable=task_to_fail,
    )

    query_input_poi >> input_merge

    [
        input_merge,
        query_grouping_id,
        event_input_task,
    ] >> mark_success_to_run_post_input_preparation

    # return send_data_to_sns
    return mark_success_to_run_post_input_preparation
