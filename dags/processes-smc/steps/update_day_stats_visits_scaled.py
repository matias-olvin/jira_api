import pandas as pd
import pendulum
from airflow.models import DAG, TaskInstance, Variable
from common.operators.bigquery import OlvinBigQueryOperator
from common.operators.exceptions import MarkSuccessOperator


def register(start: TaskInstance, dag: DAG) -> TaskInstance:

    create_day_stats_visits_scaled = OlvinBigQueryOperator(
        task_id="create_day_stats_visits_scaled",
        query="{% include './include/bigquery/daily_stats/create_daily_stats_visits_scaled.sql' %}",
    )
    start >> create_day_stats_visits_scaled

    end_date = Variable.get("smc_start_date")

    dates_compute = pd.date_range("2018-01-01", end_date, freq="YS", inclusive="left")

    dates_compute = dates_compute.append(
        pd.DatetimeIndex([pendulum.from_format(end_date, "YYYY-MM-DD").date()])
    )

    query_day_stats_visits_scaled_params = list()
    for timestamp_start, timestamp_end in zip(dates_compute[:-1], dates_compute[1:]):
        date_start = timestamp_start.strftime("%Y-%m-%d")
        date_end = timestamp_end.strftime("%Y-%m-%d")

        query_day_stats_visits_scaled_params.append(
            {"date_start": date_start, "date_end": date_end}
        )

    query_day_stats_visits_scaled = OlvinBigQueryOperator.partial(
        project_id="{{ var.value.dev_project_id }}",
        task_id="query_day_stats_visits_scaled",
        billing_tier="highest",
        query="{% include './include/bigquery/daily_stats/daily_stats_visits_scaled.sql' %}",
    ).expand(params=query_day_stats_visits_scaled_params)

    create_day_stats_visits_scaled >> query_day_stats_visits_scaled

    check_day_stats_visits_scaled_end = MarkSuccessOperator(
        task_id="check_day_stats_visits_scaled_end",
    )

    query_day_stats_visits_scaled >> check_day_stats_visits_scaled_end

    return check_day_stats_visits_scaled_end
