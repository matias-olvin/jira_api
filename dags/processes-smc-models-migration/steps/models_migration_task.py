from __future__ import annotations

from typing import List

from airflow.models import DAG, TaskInstance
from airflow.utils.task_group import TaskGroup
from airflow.providers.google.cloud.transfers.bigquery_to_bigquery import (
    BigQueryToBigQueryOperator,
)

def register(start: TaskInstance, dag: DAG, items_to_migrate: List[str]) -> List[TaskInstance]:
    with TaskGroup(group_id="migrate_models_tasks") as group:

        models_migration_tasks = list()

        for item in items_to_migrate:
            dataset, model = item.split(".")

            migrate = BigQueryToBigQueryOperator(
                task_id=f"migrate_smc_{dataset}_{model}",
                source_project_dataset_tables=f"{{{{ var.value.env_project }}}}.{{{{ params['smc_{dataset}'] }}}}.{{{{ params['{model}'] }}}}",
                destination_project_dataset_table=f"{{{{ var.value.env_project }}}}.{{{{ params['{dataset}'] }}}}.{{{{ params['{model}'] }}}}",
                write_disposition="WRITE_TRUNCATE",
                create_disposition="CREATE_IF_NEEDED",
                labels={
                    "pipeline": "{{ dag.dag_id }}",
                    "task_id": "{{ task.task_id.lower()[:63] |  replace('.','-') }}",
                }
            )

            models_migration_tasks.append(migrate)

        start >> models_migration_tasks

    return group