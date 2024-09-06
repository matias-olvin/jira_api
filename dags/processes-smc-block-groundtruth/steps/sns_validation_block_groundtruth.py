import pendulum
from airflow.models import DAG, TaskInstance
from airflow.operators.bash_operator import BashOperator
from airflow.utils.task_group import TaskGroup
from common.operators.bigquery import OlvinBigQueryOperator
from common.operators.exceptions import MarkSuccessOperator
from common.operators.validations import VolumeValidationOperator


def register(start: TaskInstance, dag: DAG, env: str) -> TaskGroup:
    gtvm_columns = dag.params["block_groundtruth_columns"]

    sns_block_groundtruth_avg_visits_params = list()
    for column in gtvm_columns:
        sns_block_groundtruth_avg_visits_params.append(
            {
                "sns_gtvm_column": column,
            },
        )

    query_visits_for_sns_block_groundtruth = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
        task_id="query_visits_for_sns_block_groundtruth",
        query="{% include './include/bigquery/sns_validation_block_ground_truth/visits_for_sns_block_groundtruth.sql' %}",
        billing_tier="high",

    )

    query_visits_for_sns_block_groundtruth_avg_visits = OlvinBigQueryOperator.partial(
            project_id="{{ var.value.dev_project_id }}",
        task_id="query_visits_for_sns_block_groundtruth_avg_visits",
        query="{% include './include/bigquery/sns_validation_block_ground_truth/query_visits_for_sns_block_groundtruth_avg_visits.sql' %}",
    ).expand(params=sns_block_groundtruth_avg_visits_params)

    (
        start
        >> query_visits_for_sns_block_groundtruth
        >> query_visits_for_sns_block_groundtruth_avg_visits
    )

    with TaskGroup(group_id="sns_validation_block_groundtruth_task_group") as group:
        for column in gtvm_columns:
            trigger_smc_validation_block_groundtruth_column = BashOperator(
                task_id=f"trigger_smc_validation_block_groundtruth_{column}",
                bash_command="gcloud composer environments run prod-sensormatic "
                "--project {{ params['sns_project'] }} "
                "--location europe-west1 "
                "--impersonate-service-account {{ params['cross_project_service_account'] }} "
                f"dags trigger -- -e '{{{{ ds }}}} {pendulum.now().time()}' "
                f"--conf '{{"
                f'"step": "{column}", '
                '"source_table": "{{ params[\'visits_daily_block_groundtruth_table\'] }}", '
                f'"pipeline": "smc_block_groundtruth", '
                f'"env": "{env}"'
                f"}}' "
                "geoscaling_validation "
                "|| true ",  # this line is needed due to gcloud bug.
            )

            run_volume_validation = VolumeValidationOperator(
                task_id=f"{column}_volume_block_3",
                sub_validation="group",
                env=env,
                pipeline="smc",
                step=column,
                run_date="{{ ds }}",
                source=f"{{{{ var.value.env_project }}}}.{{{{ params['accessible_by_sns_dataset'] }}}}.{{{{ params['visits_daily_block_groundtruth_table'] }}}}_avg_visits_{column}",
                destination="{{ params['sns_project'] }}.{{ params['accessible_by_olvin_dataset'] }}.{{ params['smc_ranking_validation_table'] }}",
                classes=["region", "naics_code", "fk_sgbrands"],
            )

            mark_success_to_complete_smc_validation_column = MarkSuccessOperator(
                task_id=f"mark_success_to_complete_smc_validation_{column}",
            )

            (
                query_visits_for_sns_block_groundtruth_avg_visits
                >> [
                    trigger_smc_validation_block_groundtruth_column,
                    run_volume_validation,
                ]
                >> mark_success_to_complete_smc_validation_column
            )

    return group
