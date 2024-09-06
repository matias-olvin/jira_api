import pandas as pd
import pendulum
from airflow.models import DAG, TaskInstance, Variable
from airflow.utils.task_group import TaskGroup
from common.operators.bigquery import OlvinBigQueryOperator
from common.operators.exceptions import MarkSuccessOperator


def register(start: TaskInstance, dag: DAG) -> TaskInstance:
    years_compute = pd.date_range(
        "2018-01-01", Variable.get("smc_demand_start_date"), freq="YS", inclusive="left"
    )
    naics_filtering_params = []
    for year_start in years_compute:

        naics_filtering_params.append({"year_name": year_start.strftime("%Y")})

    with TaskGroup(group_id="naics_filtering_task_group") as group:
        query_naics_filtering = OlvinBigQueryOperator.partial(
            task_id="smc_query_naics_filtering",
            query="{% include './include/bigquery/smc/naics_filtering.sql' %}",
        ).expand(params=naics_filtering_params)

        start >> query_naics_filtering

        naics_filtering_end = MarkSuccessOperator(task_id="smc_check_naics_filtering_end")

        query_naics_filtering >> naics_filtering_end

    return group
