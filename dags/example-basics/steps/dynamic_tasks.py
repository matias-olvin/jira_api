import calendar
from datetime import date
from typing import List, Union

from airflow.models import DAG, TaskInstance
from airflow.operators.dummy import DummyOperator
from common.operators.bigquery import OlvinBigQueryOperator


def _list_days_in_month(year: Union[str, int], month: Union[str, int]) -> List[str]:
    if isinstance(year, str) or isinstance(month, str):
        year = int(year)
        month = int(month)

    nb_days = calendar.monthrange(int(year), int(month))[1]

    return [
        date(year, month, day).strftime("%Y-%m-%d") for day in range(1, nb_days + 1)
    ]


def register(start: TaskInstance, dag: DAG, year: int, month: int) -> TaskInstance:
    params = [{"date": day} for day in _list_days_in_month(year, month)]

    dynamic_tasks = OlvinBigQueryOperator.partial(
        task_id="example_bigquery_task",
        query="{% include './include/bigquery/example_query.sql' %}",
    ).expand(params=params)

    end = DummyOperator(task_id="dynamic_tasks_end")

    start >> dynamic_tasks >> end

    return end
