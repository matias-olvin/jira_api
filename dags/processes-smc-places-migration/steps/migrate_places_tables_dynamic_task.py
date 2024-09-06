from __future__ import annotations

from typing import List

from airflow.models import DAG, TaskInstance
from airflow.operators.empty import EmptyOperator
from common.operators.bigquery import OlvinBigQueryOperator
from common.utils import queries


def register(start: TaskInstance, dag: DAG, tables: List[str]) -> TaskInstance:

    _queries = list()
    for table_dataset in tables:
        table, dataset = table_dataset.split(".")
        source = f"{{{{ var.value.env_project }}}}.{{{{ params['smc_{dataset}'] }}}}.{{{{ params['{table}'] }}}}"
        destination = f"{{{{ var.value.env_project }}}}.{{{{ params['{dataset}'] }}}}.{{{{ params['{table}'] }}}}"

        _queries.append(queries.copy_table(source, destination))

    migrate_task = OlvinBigQueryOperator.partial(
            project_id="{{ var.value.dev_project_id }}",
        task_id="migrate_places_tables"
    ).expand(query=_queries)

    end = EmptyOperator(task_id="end")

    start >> migrate_task >> end

    return end
