from __future__ import annotations

from airflow.models import TaskInstance
from airflow.utils.task_group import TaskGroup
from common.operators.bigquery import OlvinBigQueryOperator
from common.operators.exceptions import MarkSuccessOperator
from common.utils import callbacks, slack_users


def register(start: TaskInstance) -> TaskGroup:
    """
    Register tasks on the dag

    Args:
        start (airflow.models.TaskInstance): Task instance all tasks will
        be registered downstream from.

    Returns:
        airflow.utils.task_group.TaskGroup: The task group in this section.
    """
    with TaskGroup(group_id="places-tasks") as group:
        migrate_history_to_smc = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="places-migrate-history-to-smc",
            query="{% include './include/bigquery/places/places-migrate-history-to-smc.sql' %}",
        )
        create_dynamic = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="places-create-dynamic",
            query="{% include './include/bigquery/places/places-create-dynamic.sql' %}",
        )
        update_dynamic = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="places-update-dynamic",
            query="{% include './include/bigquery/places/places-update-dynamic.sql' %}",
            billing_tier="med",
        )
        create_week_array_table = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="places-create-week-array",
            query="{% include './include/bigquery/places/places-create-week-array.sql' %}",
        )
        create_site_id_lineage = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="places-site-id-lineage",
            query="{% include './include/bigquery/places/places-create-site-id-lineage.sql' %}",
        )
        create_dynamic_placekey = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="places-create-dynamic-placekey",
            query="{% include './include/bigquery/places/places-create-dynamic-placekey.sql' %}",
        )
        update_naics_code_smc_config = MarkSuccessOperator(
            task_id="update_naics_code_smc_config",
            on_failure_callback=callbacks.task_fail_slack_alert(
                "U034YDXAD1R", "U03BANPLXJR", slack_users.MATIAS, channel="prod"
            ),
        )
        update_smc_process = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="places-update-smc-process",
            query="{% include './include/bigquery/places/places-update-smc-process.sql' %}",
        )
        migrate_manual_places_to_smc = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="places-migrate-manual-places-to-smc",
            query="{% include './include/bigquery/places/places-migrate-manual-places-to-smc.sql' %}",
        )
        update_manual_places = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="places-update-manual-places",
            query="{% include './include/bigquery/places/places-update-manual-places.sql' %}",
        )

        merge_manual_places_to_places_dynamic = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="places-merge-manual-places-to-places-dynamic",
            query="{% include './include/bigquery/places/places-merge-manual-places-to-places-dynamic.sql' %}",
        )

        (
            start
            >> migrate_history_to_smc
            >> create_dynamic
            >> update_dynamic
            >> migrate_manual_places_to_smc
            >> update_manual_places
            >> merge_manual_places_to_places_dynamic
            >> [
                create_site_id_lineage,
                create_week_array_table,
                create_dynamic_placekey,
            ]
            >> update_naics_code_smc_config
            >> update_smc_process
        )

    return group
