import pandas as pd
import pendulum
from airflow.models import DAG, TaskInstance, Variable
from common.operators.bigquery import OlvinBigQueryOperator
from common.operators.exceptions import MarkSuccessOperator


def register(start: TaskInstance, dag: DAG) -> TaskInstance:


    end_date = Variable.get("smc_demand_end_date")

    dates_compute = pd.date_range(Variable.get("smc_demand_start_date"), end_date, freq="YS", inclusive="left")

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
        task_id="post_smc_query_day_stats_visits_scaled",
        query="{% include './include/bigquery/post_smc/daily_stats_visits_scaled.sql' %}",
    ).expand(params=query_day_stats_visits_scaled_params)

    start >> query_day_stats_visits_scaled

    check_day_stats_visits_scaled_end = MarkSuccessOperator(
        task_id="post_smc_check_day_stats_visits_scaled_end",
    )

    query_day_stats_visits_scaled >> check_day_stats_visits_scaled_end

    return check_day_stats_visits_scaled_end
