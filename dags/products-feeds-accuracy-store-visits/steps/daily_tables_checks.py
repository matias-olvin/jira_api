from typing import Union

from airflow.models import DAG, TaskInstance
from airflow.utils.task_group import TaskGroup
from common.operators.bigquery import OlvinBigQueryOperator


def register(start: Union[TaskInstance, TaskGroup], dag: DAG) -> TaskGroup:

    with TaskGroup(group_id="daily_feeds_checks") as group:

        check_nulls_in_descriptor_columns = OlvinBigQueryOperator(
            task_id="check_nulls_in_descriptor_columns",
            query="{% include './bigquery/data_quality/store_visits_daily_checks/check_nulls_in_descriptor_columns.sql' %}",
            billing_tier="high",
        )

        check_num_items_in_hourly_visits_list = OlvinBigQueryOperator(
            task_id="check_num_items_in_hourly_visits_list",
            query="{% include './bigquery/data_quality/store_visits_daily_checks/check_num_items_in_hourly_visits_list.sql' %}",
            billing_tier="high",
        )

        check_sum_hourly_visits_equals_daily_visits = OlvinBigQueryOperator(
            task_id="check_sum_hourly_visits_equals_daily_visits",
            query="{% include './bigquery/data_quality/store_visits_daily_checks/check_sum_hourly_visits_equals_daily_visits.sql' %}",
            billing_tier="high",
        )

        check_unique_local_date_with_store_id = OlvinBigQueryOperator(
            task_id="check_unique_local_date_with_store_id",
            query="{% include './bigquery/data_quality/store_visits_daily_checks/check_unique_local_date_with_store_id.sql' %}",
            billing_tier="high",
        )

        start >> [
            check_nulls_in_descriptor_columns,
            check_num_items_in_hourly_visits_list,
            check_sum_hourly_visits_equals_daily_visits,
            check_unique_local_date_with_store_id,
        ]

    return group
