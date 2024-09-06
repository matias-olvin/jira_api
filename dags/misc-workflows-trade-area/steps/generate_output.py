"""
DAG ID: demographics_pipeline
"""
from common.operators.bigquery import OlvinBigQueryOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import TaskInstance, DAG
from common.operators.exceptions import MarkSuccessOperator
from common.utils import callbacks


def register(start: TaskInstance, dag: DAG) -> TaskGroup:

    with TaskGroup(group_id="trade_area_output") as group:
        
        query_load_output = OlvinBigQueryOperator(
        task_id="query_load_output",
        query="{% include './include/bigquery/output/load.sql' %}",
        dag=dag,
        )
        start >> query_load_output

        query_functional_areas_30 = OlvinBigQueryOperator(
        task_id="query_functional_areas_30",
        query="{% include './include/bigquery/output/functional_areas_30.sql' %}",
        billing_tier="high",
        dag=dag,
        )

        query_functional_areas_50 = OlvinBigQueryOperator(
        task_id="query_functional_areas_50",
        query="{% include './include/bigquery/output/functional_areas_50.sql' %}",
        billing_tier="high",
        dag=dag,
        )

        query_functional_areas_70 = OlvinBigQueryOperator(
        task_id="query_functional_areas_70",
        query="{% include './include/bigquery/output/functional_areas_70.sql' %}",
        billing_tier="high",
        dag=dag,
        )

        query_functional_areas_raw = OlvinBigQueryOperator(
        task_id="query_functional_areas_raw",
        query="{% include './include/bigquery/output/functional_areas_raw.sql' %}",
        billing_tier="high",
        dag=dag,
        )

        query_functional_areas_processed = OlvinBigQueryOperator(
        task_id="query_functional_areas_processed",
        query="{% include './include/bigquery/output/functional_areas_processed.sql' %}",
        billing_tier="high",
        dag=dag,
        )

        query_functional_areas_processed_update = OlvinBigQueryOperator(
        task_id="query_functional_areas_processed_update",
        query="{% include './include/bigquery/output/functional_areas_processed_update.sql' %}",
        billing_tier="high",
        dag=dag,
        )

        check_trade_areas = MarkSuccessOperator(
            task_id="check_trade_areas",
            on_success_callback=callbacks.task_end_custom_slack_alert(
                "U05FN3F961X", msg_title="*Trade areas* created."
            ),
        )

        query_load_output >> [query_functional_areas_30,query_functional_areas_50,query_functional_areas_70] >> query_functional_areas_raw
        query_functional_areas_raw >> query_functional_areas_processed >> query_functional_areas_processed_update
        query_functional_areas_processed_update >> check_trade_areas

    return group
