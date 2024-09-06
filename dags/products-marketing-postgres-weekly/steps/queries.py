from __future__ import annotations

from airflow.models import TaskInstance
from airflow.operators.empty import EmptyOperator
from common.operators.bigquery import OlvinBigQueryOperator


def register(start: TaskInstance) -> TaskInstance:

    end = EmptyOperator(task_id="end")

    marketing_table = OlvinBigQueryOperator(
        task_id="marketing_table",
        query="{% include './include/bigquery/marketing_table.sql' %}",
    )
    

    (
        start
        >> [
            marketing_table,
        ]
        >> end
    )

    return end
