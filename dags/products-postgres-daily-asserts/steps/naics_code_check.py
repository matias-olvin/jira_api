from __future__ import annotations

from airflow.models import TaskInstance
from airflow.operators.empty import EmptyOperator
from common.operators.bigquery import OlvinBigQueryOperator


def register(start: TaskInstance) -> TaskInstance:

    end = EmptyOperator(task_id="end")

    naics_code_check_in_postgres_batch_sgbrandraw_table = OlvinBigQueryOperator(
        task_id="naics_code_check_in_postgres_batch_sgbrandraw_table",
        query="{% include './include/bigquery/naics_code_check_in_postgres_batch_sgbrandraw_table.sql' %}",
    )

    naics_code_check_in_postgres_batch_sgplaceraw_table = OlvinBigQueryOperator(
        task_id="naics_code_check_in_postgres_batch_sgplaceraw_table",
        query="{% include './include/bigquery/naics_code_check_in_postgres_batch_sgplaceraw_table.sql' %}",
    )

    naics_code_check_in_postgres_sgbrandraw_table = OlvinBigQueryOperator(
        task_id="naics_code_check_in_postgres_sgbrandraw_table",
        query="{% include './include/bigquery/naics_code_check_in_postgres_sgbrandraw_table.sql' %}",
    )

    naics_code_check_in_postgres_sgplaceraw_table = OlvinBigQueryOperator(
        task_id="naics_code_check_in_postgres_sgplaceraw_table",
        query="{% include './include/bigquery/naics_code_check_in_postgres_sgplaceraw_table.sql' %}",
    )

    (
        start
        >> [
            naics_code_check_in_postgres_batch_sgbrandraw_table,
            naics_code_check_in_postgres_batch_sgplaceraw_table,
            naics_code_check_in_postgres_sgbrandraw_table,
            naics_code_check_in_postgres_sgplaceraw_table,
        ]
        >> end
    )

    return end
