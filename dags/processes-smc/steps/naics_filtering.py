import pandas as pd
import pendulum
from airflow.models import DAG, TaskInstance
from airflow.utils.task_group import TaskGroup
from common.operators.bigquery import OlvinBigQueryOperator
from common.operators.exceptions import MarkSuccessOperator


def register(start: TaskInstance, dag: DAG) -> TaskInstance:
    years_compute = pd.date_range(
        "2018-01-01", pendulum.today().strftime("%Y-%m-%d"), freq="YS", inclusive="left"
    )
    naics_filtering_params = []
    for year_start in years_compute:

        naics_filtering_params.append({"year_name": year_start.strftime("%Y")})

    with TaskGroup(group_id="naics_filtering_task_group") as group:
        query_naics_filtering = OlvinBigQueryOperator.partial(
            project_id="{{ var.value.dev_project_id }}",
            task_id="query_naics_filtering",
            billing_tier="highest",
            query="{% include './include/bigquery/naics_filtering.sql' %}",
        ).expand(params=naics_filtering_params)

        start >> query_naics_filtering

        naics_filtering_end = MarkSuccessOperator(task_id="check_naics_filtering_end")

        query_naics_filtering >> naics_filtering_end

    return group