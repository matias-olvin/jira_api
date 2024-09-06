from __future__ import annotations

from typing import List

import pandas as pd
import pendulum
from airflow.models import DAG, TaskInstance
from common.operators.bigquery import OlvinBigQueryOperator
from common.utils import queries


def register(start: TaskInstance, dag: DAG, datasets: List[str]) -> TaskInstance:
    
    years_compute = pd.date_range(
        "2018-01-01", pendulum.now().format("YYYY-MM-DD"), freq="YS", inclusive="left"
    )

    _queries = list()

    for dataset in datasets:
        for year_start in years_compute:
            table = f"{pendulum.instance(year_start).year}"

            source = f"{{{{ var.value.env_project }}}}.{{{{ params['smc_{dataset}'] }}}}.{table}"
            destination = f"{{{{ var.value.env_project }}}}.{{{{ params['{dataset}'] }}}}.{table}"

            _queries.append(queries.copy_table(source, destination))

    migrate_tables = OlvinBigQueryOperator.partial(
            project_id="{{ var.value.dev_project_id }}",task_id="migrate_visits_tables").expand(query=_queries)

    start >> migrate_tables

    return migrate_tables

