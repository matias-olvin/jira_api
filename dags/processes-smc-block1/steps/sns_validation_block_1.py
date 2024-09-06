
import pendulum
from airflow.models import DAG, TaskInstance
from airflow.operators.bash_operator import BashOperator
from airflow.utils.task_group import TaskGroup
from common.operators.bigquery import OlvinBigQueryOperator
from common.operators.exceptions import MarkSuccessOperator


def register(start: TaskInstance, dag: DAG) -> TaskGroup:
    with TaskGroup(group_id="visits_for_sns_block_1_task_group") as group:
        query_visits_for_sns_block_1 = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="query_visits_for_sns_block_1",
            query="{% include './include/bigquery/visits_for_sns_block_1.sql' %}",
            billing_tier="high"
        )
        start >> query_visits_for_sns_block_1

        for column in dag.params["block_1_columns"]:
            trigger_smc_validation_block_1_column = BashOperator(
                task_id=f"trigger_smc_validation_block_1_{column}",
                bash_command=f"gcloud composer environments run prod-sensormatic "
                "--project {{ params['sns_project'] }} "
                "--location europe-west1 "
                "--impersonate-service-account {{ params['cross_project_service_account'] }} "
                f"dags trigger -- -e '{{{{ ds }}}} {pendulum.now().time()}' "
                f"--conf '{{"
                f'"step": "{column}", '
                "\"source_table\": \"{{ params['visits_daily_block_1_table'] }}\", "
                f'"pipeline": "smc_block_1", '
                f'"env": "prod"'
                f"}}' "
                f"geoscaling_validation "
                "|| true ",  # this line is needed due to gcloud bug.
            )

            mark_success_to_complete_smc_validation_column = MarkSuccessOperator(
                task_id=f"mark_success_to_complete_smc_validation_{column}",
            )

            (
                query_visits_for_sns_block_1
                >> trigger_smc_validation_block_1_column
                >> mark_success_to_complete_smc_validation_column
            )        

    return group
