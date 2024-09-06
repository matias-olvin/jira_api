from __future__ import annotations

from airflow.models import TaskInstance
from airflow.utils.task_group import TaskGroup
from common.operators.bigquery import OlvinBigQueryOperator, SNSBigQueryOperator
from common.utils import formatting, queries


def register( start: TaskInstance, dag) -> TaskInstance:
    source = "olvin-project.example.example_input"
    destination = "sns-project.example.example_input"
    staging = formatting.accessible_table_name_format("sns", source)

    with TaskGroup(group_id="copy_to_sns") as group:
        copy_to_accessible = OlvinBigQueryOperator(
            task_id="copy_to_accessible_by_sns",
            query=queries.copy_table(source, staging),
        )
        copy_to_working = SNSBigQueryOperator(
            task_id="copy_to_working_sns",
            query=queries.copy_table(staging, destination),
        )
        drop_staging = OlvinBigQueryOperator(
            task_id="delete_from_accessible_by_sns",
            query=queries.drop_table(staging),
        )

        start >> copy_to_accessible
        copy_to_accessible >> copy_to_working
        copy_to_working >> drop_staging

    return group
