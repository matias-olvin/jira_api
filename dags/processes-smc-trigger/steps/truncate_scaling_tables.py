from airflow.models import DAG, TaskInstance, Variable
from typing import List
from common.operators.bigquery import OlvinBigQueryOperator
import pandas as pd

def register(start: TaskInstance, dag: DAG, dataset_to_truncate: List[str]) -> TaskInstance:

    years_compute = pd.date_range(
        "2018-01-01", Variable.get("smc_start_date"), freq="YS", inclusive="left"
    )

    truncate_tables_params = list()

    for year_start in years_compute:
        year_name = year_start.strftime("%Y")

        for dataset in dataset_to_truncate:

            truncate_tables_params.append(
                {
                    "scaling_dataset": dag.params[dataset],
                    "year_name": year_name,
                }
            )

    truncate_tables_task = OlvinBigQueryOperator.partial(
            project_id="{{ var.value.dev_project_id }}",
        task_id="truncate_scaling_tables",
        query="{% include './include/bigquery/truncate_scaling_tables.sql' %}"
    ).expand(params=truncate_tables_params)

    start >> truncate_tables_task

    return truncate_tables_task