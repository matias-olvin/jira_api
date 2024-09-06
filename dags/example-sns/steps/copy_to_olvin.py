from __future__ import annotations

from airflow.models import TaskInstance
from airflow.utils.task_group import TaskGroup
from common.operators.bigquery import OlvinBigQueryOperator, SNSBigQueryOperator
from common.utils import formatting, queries


def register(start: TaskInstance, dag) -> TaskInstance:
    source = "sns-vendor-olvin-poc.example.example_output"
    destination = "storage-prod-olvin-com.example.example_output"
    staging = formatting.accessible_table_name_format("olvin", destination)

    with TaskGroup(group_id="copy_to_olvin") as group:
        copy_to_accessible = SNSBigQueryOperator(
            task_id="copy_to_accessible_by_olvin",
            query=queries.copy_table(source, staging),
        )
        copy_to_working = OlvinBigQueryOperator(
            task_id="copy_to_working_olvin",
            query=queries.copy_table(staging, destination),
        )
        drop_staging = SNSBigQueryOperator(
            task_id="delete_from_accessible_by_olvin",
            query=queries.drop_table(staging),
        )

        start >> copy_to_accessible
        copy_to_accessible >> copy_to_working
        copy_to_working >> drop_staging

    return group
