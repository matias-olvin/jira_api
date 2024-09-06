from __future__ import annotations

from typing import List

from airflow.models import DAG, TaskInstance
from airflow.operators.empty import EmptyOperator

from common.utils import queries
from common.operators.bigquery import OlvinBigQueryOperator

def register(start: TaskInstance, dag: DAG, tables: List[str]) -> TaskInstance:

    _queries = list()
    for table in tables:
        source = f"{{{{ var.value.env_project }}}}.{{{{ params['smc_metrics_dataset'] }}}}.{{{{ params['{table}'] }}}}"
        destination = f"{{{{ var.value.env_project }}}}.{{{{ params['metrics_dataset'] }}}}.{{{{ params['{table}'] }}}}"

        _queries.append(queries.copy_table(source, destination))

    end = EmptyOperator(task_id="end", dag=dag)

    metrics_migration_task = OlvinBigQueryOperator.partial(
            project_id="{{ var.value.dev_project_id }}",task_id="migrate_metrics_tables").expand(query=_queries)

    start >> metrics_migration_task >> end

    return end