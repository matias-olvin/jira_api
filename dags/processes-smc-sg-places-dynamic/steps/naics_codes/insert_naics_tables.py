from __future__ import annotations

from airflow.models import TaskInstance
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from common.operators.bigquery import OlvinBigQueryOperator
from common.operators.exceptions import MarkSuccessOperator


def register(start: TaskInstance) -> TaskGroup:
    with TaskGroup(group_id="insert_naics_codes_tables_task_group") as group:
        create_snapshot_different_digit_naics_code = OlvinBigQueryOperator(
            task_id="create_snapshot_different_digit_naics_code",
            query="{% include './include/bigquery/naics_codes/snapshot_creations/create_snapshot_different_digit_naics_code.sql' %}",
        )

        create_snapshot_naics_code_subcategories = OlvinBigQueryOperator(
            task_id="create_snapshot_naics_code_subcategories",
            query="{% include './include/bigquery/naics_codes/snapshot_creations/create_snapshot_naics_code_subcategories.sql' %}",
        )

        create_snapshot_sg_categories_match = OlvinBigQueryOperator(
            task_id="create_snapshot_sg_categories_match",
            query="{% include './include/bigquery/naics_codes/snapshot_creations/create_snapshot_sg_categories_match.sql' %}",
        )

        update_different_digit_naics_code = OlvinBigQueryOperator(
            task_id="update_different_digit_naics_code",
            query="{% include './include/bigquery/naics_codes/insert_naics_tables/update_different_digit_naics_code.sql' %}",
        )

        update_naics_code_subcategories = OlvinBigQueryOperator(
            task_id="update_naics_code_subcategories",
            query="{% include './include/bigquery/naics_codes/insert_naics_tables/update_naics_code_subcategories.sql' %}",
        )

        update_sg_categories_match = OlvinBigQueryOperator(
            task_id="update_sg_categories_match",
            query="{% include './include/bigquery/naics_codes/insert_naics_tables/update_sg_categories_match.sql' %}",
        )

        check_staging_tables_before_insert = MarkSuccessOperator(
            task_id="check_staging_tables_before_insert"
        )

        create_snapshots_end = EmptyOperator(task_id="create_snapshots_end")

        update_naics_codes_end = EmptyOperator(task_id="update_naics_codes_end")

        (
            start
            >> check_staging_tables_before_insert
            >> [
                create_snapshot_different_digit_naics_code,
                create_snapshot_naics_code_subcategories,
                create_snapshot_sg_categories_match,
            ]
            >> create_snapshots_end
            >> [
                update_different_digit_naics_code,
                update_naics_code_subcategories,
                update_sg_categories_match,
            ]
            >> update_naics_codes_end
        )

    return group
