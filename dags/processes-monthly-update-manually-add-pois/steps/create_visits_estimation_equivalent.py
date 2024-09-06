from airflow.models import DAG, TaskInstance
from airflow.utils.task_group import TaskGroup
from common.operators.bigquery import OlvinBigQueryOperator


def register(start: TaskInstance, dag: DAG) -> TaskInstance:
    with TaskGroup(group_id="format_hourly_monthly_daily_visits_group") as group:
        create_visits_estimation_equivalent_daily = OlvinBigQueryOperator(
            task_id="create_visits_estimation_equivalent_daily",
            query="{% include './include/bigquery/create_visits_estimation_equivalent/create_visits_estimation_equivalent_daily.sql' %}",
        )

        format_daily_visits = OlvinBigQueryOperator(
            task_id="format_daily_visits",
            query="{% include './include/bigquery/create_visits_estimation_equivalent/format_daily_visits.sql' %}",
        )

        format_monthly_visits = OlvinBigQueryOperator(
            task_id="format_monthly_visits",
            query="{% include './include/bigquery/create_visits_estimation_equivalent/format_monthly_visits.sql' %}",
        )

        create_visits_estimation_equivalent_hourly = OlvinBigQueryOperator(
            task_id="create_visits_estimation_equivalent_hourly",
            query="{% include './include/bigquery/create_visits_estimation_equivalent/create_visits_estimation_equivalent_hourly.sql' %}",
        )

        format_hourly_all_visits = OlvinBigQueryOperator(
            task_id="format_hourly_all_visits",
            query="{% include './include/bigquery/create_visits_estimation_equivalent/format_hourly_all_visits.sql' %}",
        )

        format_hourly_visits = OlvinBigQueryOperator(
            task_id="format_hourly_visits",
            query="{% include './include/bigquery/create_visits_estimation_equivalent/format_hourly_visits.sql' %}",
        )

        start >> create_visits_estimation_equivalent_daily

        create_visits_estimation_equivalent_daily >> [
            format_daily_visits,
            format_monthly_visits,
            create_visits_estimation_equivalent_hourly,
        ]

        (
            create_visits_estimation_equivalent_hourly
            >> format_hourly_all_visits
            >> format_hourly_visits
        )

    return group
