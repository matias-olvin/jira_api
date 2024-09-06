import pendulum
from airflow.models import DAG, TaskInstance
from airflow.operators.bash_operator import BashOperator
from airflow.utils.task_group import TaskGroup
from common.operators.bigquery import OlvinBigQueryOperator
from common.operators.exceptions import MarkSuccessOperator
from common.operators.validations import VolumeValidationOperator


def register(start: TaskInstance, dag: DAG, env: str) -> TaskGroup:
    with TaskGroup(
        group_id="sns_validation_block_daily_estimation_task_group"
    ) as group:

        block = "daily_estimation"

        query_visits_for_sns_block_daily_estimation = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="query_visits_for_sns_block_daily_estimation",
            query="{% include './include/bigquery/visits_for_sns_block_daily_estimation.sql' %}",
            billing_tier="high",
        )
        start >> query_visits_for_sns_block_daily_estimation

        for column in dag.params["block_daily_estimation_columns"]:
            trigger_smc_validation_block_daily_estimation_column = BashOperator(
                task_id=f"trigger_smc_validation_block_daily_estimation_{column}",
                bash_command=f"gcloud composer environments run prod-sensormatic "
                "--project {{ params['sns_project'] }} "
                "--location europe-west1 "
                "--impersonate-service-account {{ params['cross_project_service_account'] }} "
                f"dags trigger -- -e '{{{{ ds }}}} {pendulum.now().time()}' "
                f"--conf '{{"
                f'"step": "{column}", '
                '"source_table": "{{ params[\'visits_daily_block_daily_estimation_table\'] }}", '
                f'"pipeline": "smc_block_daily_estimation", '
                f'"env": "{env}"'
                f"}}' "
                f"geoscaling_validation "
                "|| true ",  # this line is needed due to gcloud bug.
            )
            (
                query_visits_for_sns_block_daily_estimation
                >> trigger_smc_validation_block_daily_estimation_column
            )

            run_volume_validation = VolumeValidationOperator(
                task_id=f"{column}_volume_{block}",
                sub_validation="group",
                env=env,
                pipeline="smc",
                step=column,
                run_date="{{ ds }}",
                source="{{ var.value.env_project }}.{{ params['accessible_by_sns_dataset'] }}.{{ params['visits_daily_block_daily_estimation_table'] }}_unnested",
                destination="{{ params['sns_project'] }}.{{ params['accessible_by_olvin_dataset'] }}.{{ params['smc_ranking_validation_table'] }}",  # check if this is a table
                classes=["region", "naics_code", "fk_sgbrands"],
            )
            query_visits_for_sns_block_daily_estimation >> run_volume_validation

            mark_success_to_complete_smc_validation_column = MarkSuccessOperator(
                task_id=f"mark_success_to_complete_smc_validation_{column}",
            )

            [
                trigger_smc_validation_block_daily_estimation_column,
                run_volume_validation,
            ] >> mark_success_to_complete_smc_validation_column

    return group
