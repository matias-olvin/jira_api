from __future__ import annotations

from airflow.models import DAG, TaskInstance, Variable
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from common.operators.bigquery import OlvinBigQueryOperator


def check_uniqueness(
    *,
    dataset_bigquery: str,
    table_bigquery: str,
    database_table: str,
    params: dict[str, str],
    fk_column: str,
    date_column: str = "local_date",
):
    return OlvinBigQueryOperator(
        task_id=f"checkUniqueness_{database_table}",
        query="{% include './include/bigquery/db_check/uniqueness.sql' %}",
        params={
            **params,
            "bigquery-dataset": dataset_bigquery,
            "bigquery-table": table_bigquery,
            "fk-column": fk_column,
            "date-column": date_column,
        },
    )


def process_invalid_keys(
    *,
    dataset_bigquery: str,
    table_bigquery: str,
    database_table: str,
    comparison_table: str,
    comparison_dataset: str,
    params: dict[str, str],
    fk_column: str,
):
    return OlvinBigQueryOperator(
        task_id=f"processInvalidKeys_{database_table}",
        query="{% include './include/bigquery/db_check/process_invalid.sql' %}",
        params={
            **params,
            "bigquery-dataset": dataset_bigquery,
            "bigquery-table": table_bigquery,
            "comparison-dataset": comparison_dataset,
            "comparison-table": comparison_table,
            "fk-column": fk_column,
        },
    )


def register(start: TaskInstance, dag: DAG) -> TaskGroup:
    params = {
        "bigquery-project": (
            Variable.get("almanac_project")
            if dag.params["stage"] == "production"
            else Variable.get("env_project")
        ),
    }

    with TaskGroup("db_check_task_group", tooltip="Check database integrity") as group:
        check_uniqueness_end = EmptyOperator(task_id="checkUniqueness_end")
        process_invalid_keys_end = EmptyOperator(task_id="processInvalidKeys_end")

        for table in dag.params["uniqueness_check_input_tables_list"]:
            check_uniqueness_task = check_uniqueness(
                dataset_bigquery=dag.params["postgres-rt-dataset"],
                table_bigquery=table,
                database_table=table,
                params=params,
                fk_column="fk_sgplaces",
            )
            check_uniqueness_task >> check_uniqueness_end

        # process invalid keys
        process_invalid_keys_sgplacedailyvisitsraw = process_invalid_keys(
            dataset_bigquery=dag.params["postgres-rt-dataset"],
            table_bigquery=dag.params["sgplacedailyvisitsraw-table"],
            database_table=dag.params["sgplacedailyvisitsraw-table"],
            comparison_dataset=dag.params["postgres-batch-dataset"],
            comparison_table=dag.params["sgplaceraw-table"],
            params=params,
            fk_column="fk_sgplaces",
        )
        process_invalid_keys_sgplacemonthlyvisitsraw = process_invalid_keys(
            dataset_bigquery=dag.params["postgres-rt-dataset"],
            table_bigquery=dag.params["sgplacemonthlyvisitsraw-table"],
            database_table=dag.params["sgplacemonthlyvisitsraw-table"],
            comparison_dataset=dag.params["postgres-batch-dataset"],
            comparison_table=dag.params["sgplaceraw-table"],
            params=params,
            fk_column="fk_sgplaces",
        )
        process_invalid_keys_sgplaceranking = process_invalid_keys(
            dataset_bigquery=dag.params["postgres-rt-dataset"],
            table_bigquery=dag.params["sgplaceranking-table"],
            database_table=dag.params["sgplaceranking-table"],
            comparison_dataset=dag.params["postgres-batch-dataset"],
            comparison_table=dag.params["sgplaceraw-table"],
            params=params,
            fk_column="fk_sgplaces",
        )
        (
            check_uniqueness_end
            >> [
                process_invalid_keys_sgplacedailyvisitsraw,
                process_invalid_keys_sgplacemonthlyvisitsraw,
                process_invalid_keys_sgplaceranking,
            ]
            >> process_invalid_keys_end
        )

        start >> group

    return group
