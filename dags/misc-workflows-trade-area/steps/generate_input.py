"""
DAG ID: demographics_pipeline
"""
from common.operators.bigquery import OlvinBigQueryOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import TaskInstance, DAG


def register(start: TaskInstance, dag: DAG) -> TaskGroup:

    with TaskGroup(group_id="trade_area_input") as group:

        query_raw_homes = OlvinBigQueryOperator(
        task_id="query_raw_homes_input",
        query="{% include './include/bigquery/input/raw_homes.sql' %}",
        billing_tier="high",
        dag=dag,
        )
        start >> query_raw_homes

        query_trade_area_input = OlvinBigQueryOperator(
        task_id="query_trade_area_input",
        query="{% include './include/bigquery/input/trade_area.sql' %}",
        dag=dag,
        billing_tier="med",
        )
        query_raw_homes >> query_trade_area_input

        query_export_input_test = OlvinBigQueryOperator(
        task_id="query_export_input_test",
        query="{% include './include/bigquery/input/export_test.sql' %}",
        dag=dag,
        billing_tier="med",
        )

        query_export_input = OlvinBigQueryOperator(
        task_id="query_export_input",
        query="{% include './include/bigquery/input/export.sql' %}",
        dag=dag,
        billing_tier="med",
        )
        query_trade_area_input >> query_export_input_test >> query_export_input

   
    return group