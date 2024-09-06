from __future__ import annotations

from airflow.models import DAG, TaskInstance
from airflow.utils.task_group import TaskGroup
from common.operators.bigquery import OlvinBigQueryOperator


def register(start: TaskInstance, dag: DAG, postgres_dataset: str) -> TaskGroup:
    with TaskGroup(
        group_id=f"SGCenterBenchMarkingTables_{postgres_dataset}_task_group"
    ) as group:

        create_sgcentersbenchmarkingraw_table = OlvinBigQueryOperator(
            task_id=f"create_sgcentersbenchmarkingraw_{postgres_dataset}_table",
            query='{% with postgres_dataset="'
            f"{postgres_dataset}"
            '"%}{% include "./bigquery/sgcenterbenchmark/create_sgcentersbenchmarkingraw_table.sql" %}{% endwith %}',
        )

        create_sgstatecentersbenchmarkingraw_table = OlvinBigQueryOperator(
            task_id=f"create_sgstatecentersbenchmarkingraw_{postgres_dataset}_table",
            query='{% with postgres_dataset="'
            f"{postgres_dataset}"
            '"%}{% include "./bigquery/sgcenterbenchmark/create_sgstatecentersbenchmarkingraw_table.sql" %}{% endwith %}',
        )

        parallel_tasks = [
            create_sgcentersbenchmarkingraw_table,
            create_sgstatecentersbenchmarkingraw_table,
        ]

        start >> parallel_tasks

    return group
