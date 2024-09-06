import pendulum
from airflow.models import DAG, TaskInstance
from airflow.operators.bash_operator import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from common.operators.bigquery import OlvinBigQueryOperator
from common.operators.exceptions import MarkSuccessOperator


def register(start: TaskInstance, dag: DAG, env: str) -> TaskInstance:
    with TaskGroup(group_id="sns_validation_block_0_task_group") as group:
        query_smc_sns_poi_metadata_table = OlvinBigQueryOperator(
            task_id="query_smc_sns_poi_metadata_table",
            query="{% include './include/bigquery/block_0/sns_validation_block_0_queries/query_smc_sns_poi_metadata_table.sql' %}",
        )
        start >> query_smc_sns_poi_metadata_table

        query_smc_category_names_dynamic = OlvinBigQueryOperator(
            task_id="query_smc_category_names_dynamic",
            query="{% include './include/bigquery/block_0/sns_validation_block_0_queries/query_smc_category_names_dynamic.sql' %}",
        )
        start >> query_smc_category_names_dynamic

        query_visits_for_sns_block_0 = OlvinBigQueryOperator(
            task_id="query_visits_for_sns_block_0",
            query="{% include './include/bigquery/block_0/visits_for_sns_block_0.sql' %}",
            billing_tier="highest",
        )
        query_smc_sns_poi_metadata_table >> query_visits_for_sns_block_0

        trigger_smc_validation_reference = BashOperator(
            task_id="trigger_smc_validation_reference",
            bash_command=f"gcloud composer environments run prod-sensormatic "
            "--project {{ params['sns_project'] }} "
            "--location europe-west1 "
            "--impersonate-service-account {{ params['cross_project_service_account'] }} "
            f"dags trigger -- -e '{{{{ ds }}}} {pendulum.now().time()}' "
            f"--conf '{{"
            f'"step": "reference", '
            f'"pipeline": "smc", '
            f'"env": "{env}"'
            f"}}' "
            f"geoscaling_validation_reference "
            "|| true ",  # this line is needed due to gcloud bug.
        )
        [
            query_smc_sns_poi_metadata_table,
            query_smc_category_names_dynamic,
        ] >> trigger_smc_validation_reference

        # wait_for_completion_task
        wait_for_smc_validation_reference = EmptyOperator(
            task_id="wait_for_smc_validation_reference"
        )
        # move_tables_task
        move_smc_validation_reference_tables = OlvinBigQueryOperator(
            task_id="move_smc_validation_reference_tables",
            query="{% include './include/bigquery/block_0/move_smc_validation_reference_tables.sql' %}",
        )
        mark_success_to_complete_smc_validation_reference = MarkSuccessOperator(
            task_id="mark_success_to_complete_smc_validation_reference",
        )
        (
            trigger_smc_validation_reference
            >> wait_for_smc_validation_reference
            >> mark_success_to_complete_smc_validation_reference
            >> move_smc_validation_reference_tables
        )

        for column in dag.params["block_0_columns"]:
            trigger_smc_validation_block_0_column = BashOperator(
                task_id=f"trigger_smc_validation_block_0_{column}",
                bash_command=f"gcloud composer environments run prod-sensormatic "
                "--project {{ params['sns_project'] }} "
                "--location europe-west1 "
                "--impersonate-service-account {{ params['cross_project_service_account'] }} "
                f"dags trigger -- -e '{{{{ ds }}}} {pendulum.now().time()}' "
                f"--conf '{{"
                f'"step": "{column}", '
                '"source_table": "{{ params[\'visits_daily_block_0_table\'] }}", '
                f'"pipeline": "smc_block_0", '
                f'"env": "{env}"'
                f"}}' "
                f"geoscaling_validation "
                "|| true ",  # this line is needed due to gcloud bug.
            )
            [
                query_visits_for_sns_block_0,
                move_smc_validation_reference_tables,
            ] >> trigger_smc_validation_block_0_column
            mark_success_to_complete_smc_validation_column = MarkSuccessOperator(
                task_id=f"mark_success_to_complete_smc_validation_{column}",
            )
            (
                trigger_smc_validation_block_0_column
                >> mark_success_to_complete_smc_validation_column
            )

    mark_success_to_complete_sns_validation_block_0 = MarkSuccessOperator(
        task_id="mark_success_to_complete_sns_validation_block_0",
    )

    group >> mark_success_to_complete_sns_validation_block_0

    return mark_success_to_complete_sns_validation_block_0
