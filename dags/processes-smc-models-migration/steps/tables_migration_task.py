from __future__ import annotations

from typing import List

from airflow.models import DAG, TaskInstance
from airflow import AirflowException
from airflow.operators.empty import EmptyOperator

from common.utils import queries
from common.operators.bigquery import OlvinBigQueryOperator

def copy_table_queries(migration_items: List[str]):

    copy_table_queries_list = list()

    for item in migration_items:

        dataset, table = item.split(".")
    
        source = f"{{{{ var.value.env_project }}}}.{{{{ params['smc_{dataset}'] }}}}.{{{{ params['{table}'] }}}}"
        destination = f"{{{{ var.value.env_project }}}}.{{{{ params['{dataset}'] }}}}.{{{{ params['{table}'] }}}}"

        copy_table_queries_list.append(queries.copy_table(source=source, destination=destination))
    
    return copy_table_queries_list


def register(start: TaskInstance, dag: DAG, items_to_migrate: List[str]) -> TaskInstance:

    _queries = copy_table_queries(migration_items=items_to_migrate)

    migrate = OlvinBigQueryOperator.partial(
            project_id="{{ var.value.dev_project_id }}",task_id="migrate_tables").expand(query=_queries)

    start >> migrate

    return migrate