from __future__ import annotations

from airflow.models import DAG, TaskInstance
from airflow.utils.task_group import TaskGroup
from common.operators.bigquery import OlvinBigQueryOperator


def register(start: TaskInstance, dag: DAG, postgres_dataset: str) -> TaskGroup:
    with TaskGroup(
        group_id=f"SGBrandBenchMarkingTables_{postgres_dataset}_task_group"
    ) as group:

        create_sgbrandbenchmarkingraw_table = OlvinBigQueryOperator(
            task_id=f"create_sgbrandbenchmarkingraw_{postgres_dataset}_table",
            query='{% with postgres_dataset="'
            f"{postgres_dataset}"
            '"%}{% include "./bigquery/sgbrandbenchmark/create_sgbrandbenchmarkingraw_table.sql" %}{% endwith %}',
        )

        create_sgmarketbenchmarkingraw_table = OlvinBigQueryOperator(
            task_id=f"create_sgbrandmarketbenchmarkingraw_{postgres_dataset}_table",
            query='{% with postgres_dataset="'
            f"{postgres_dataset}"
            '"%}{% include "./bigquery/sgbrandbenchmark/create_sgmarketbenchmarkingraw_table.sql" %}{% endwith %}',
        )

        create_sgbrandstatebenchmarkingraw_table = OlvinBigQueryOperator(
            task_id=f"create_sgbrandstatebenchmarkingraw_{postgres_dataset}_table",
            query='{% with postgres_dataset="'
            f"{postgres_dataset}"
            '"%}{% include "./bigquery/sgbrandbenchmark/create_sgbrandstatebenchmarkingraw_table.sql" %}{% endwith %}',
        )

        create_sgstatemarketbenchmarkingraw_table = OlvinBigQueryOperator(
            task_id=f"create_sgstatemarketbenchmarkingraw_{postgres_dataset}_table",
            query='{% with postgres_dataset="'
            f"{postgres_dataset}"
            '"%}{% include "./bigquery/sgbrandbenchmark/create_sgstatemarketbenchmarkingraw_table.sql" %}{% endwith %}',
        )

        create_sgbrandcitybenchmarkingraw_table = OlvinBigQueryOperator(
            task_id=f"create_sgbrandcitybenchmarkingraw_{postgres_dataset}_table",
            query='{% with postgres_dataset="'
            f"{postgres_dataset}"
            '"%}{% include "./bigquery/sgbrandbenchmark/create_sgbrandcitybenchmarkingraw_table.sql" %}{% endwith %}',
        )

        create_sgcitymarketbenchmarkingraw_table = OlvinBigQueryOperator(
            task_id=f"create_sgcitymarketbenchmarkingraw_{postgres_dataset}_table",
            query='{% with postgres_dataset="'
            f"{postgres_dataset}"
            '"%}{% include "./bigquery/sgbrandbenchmark/create_sgcitymarketbenchmarkingraw_table.sql" %}{% endwith %}',
        )

        parallel_tasks = [
            create_sgbrandbenchmarkingraw_table,
            create_sgmarketbenchmarkingraw_table,
            create_sgbrandstatebenchmarkingraw_table,
            create_sgstatemarketbenchmarkingraw_table,
            create_sgbrandcitybenchmarkingraw_table,
            create_sgcitymarketbenchmarkingraw_table,
        ]

        start >> parallel_tasks

    return group
