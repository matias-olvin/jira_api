"""
DAG ID: demographics_pipeline
"""
from common.operators.bigquery import OlvinBigQueryOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import TaskInstance, DAG


def register(start: TaskInstance, dag: DAG) -> TaskGroup:

    with TaskGroup(group_id="trade_area_tests") as group:

        query_check_poi_drop = OlvinBigQueryOperator(
        task_id="query_check_poi_drop",
        query="{% include './include/bigquery/tests/check_poi_drop.sql' %}",
        dag=dag,
        )
        start >> query_check_poi_drop

   
    return group