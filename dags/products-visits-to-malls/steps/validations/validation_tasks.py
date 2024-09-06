from __future__ import annotations

from airflow.models import TaskInstance
from airflow.utils.task_group import TaskGroup
from common.operators.bigquery import OlvinBigQueryOperator


def register(start: TaskInstance) -> TaskGroup:
    with TaskGroup(group_id="validations") as group:

        check_duplicates_visits_to_malls_daily_task = OlvinBigQueryOperator(
            task_id="check_duplicates_visits_to_malls_daily",
            query="{% include './include/bigquery/validations/check_duplicates_visits_to_malls_daily.sql' %}",
        )

        check_nulls_visits_to_malls_daily_task = OlvinBigQueryOperator(
            task_id="check_nulls_visits_to_malls_daily",
            query="{% include './include/bigquery/validations/check_nulls_visits_to_malls_daily.sql' %}",
        )

        check_duplicates_visits_to_malls_monthly_task = OlvinBigQueryOperator(
            task_id="check_duplicates_visits_to_malls_monthly",
            query="{% include './include/bigquery/validations/check_duplicates_visits_to_malls_monthly.sql' %}",
        )

        check_nulls_visits_to_malls_monthly_task = OlvinBigQueryOperator(
            task_id="check_nulls_visits_to_malls_monthly",
            query="{% include './include/bigquery/validations/check_nulls_visits_to_malls_monthly.sql' %}",
        )

        (
            start
            >> check_duplicates_visits_to_malls_daily_task
            >> check_nulls_visits_to_malls_daily_task
            >> check_duplicates_visits_to_malls_monthly_task
            >> check_nulls_visits_to_malls_monthly_task
        )

    return group
