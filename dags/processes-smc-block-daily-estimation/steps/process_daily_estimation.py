import pendulum
from airflow.models import DAG, TaskInstance
from airflow.operators.bash_operator import BashOperator
from airflow.utils.task_group import TaskGroup
from common.operators.bigquery import OlvinBigQueryOperator
from common.operators.exceptions import MarkSuccessOperator


def register(start: TaskInstance, dag: DAG) -> TaskGroup:
    with TaskGroup(group_id="daily_estimation_model_task_group") as group:
        query_poi_list = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="query_poi_list",
            query="{% include './include/bigquery/daily_estimation_model/poi_list.sql' %}",
        )

        query_daily_olvin = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="query_daily_olvin",
            query="{% include './include/bigquery/daily_estimation_model/daily_olvin.sql' %}",
            billing_tier="high",
        )

        trigger_task = BashOperator(
            task_id="trigger_smc_daily_estimation",
            bash_command=f"gcloud composer environments run prod-sensormatic "
            "--project {{ params['sns_project'] }} "
            f"--location europe-west1 "
            "--impersonate-service-account {{ params['cross_project_service_account'] }} "
            f'dags trigger -- -e "{{{{ ds }}}} {pendulum.now().time()}" smc_daily_estimation '
            f"|| true ",  # this line is needed due to gcloud bug.
        )

        check_daily_estimation_task = MarkSuccessOperator(  # add notification?
            task_id="mark_success_to_confirm_daily_estimation",
        )

        query_load_daily_estimation_results = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="query_load_daily_estimation_results",
            query="{% include './include/bigquery/daily_estimation_model/load_daily_estimation_results.sql' %}",
        )

        (
            start
            >> query_poi_list
            >> query_daily_olvin
            >> trigger_task
            >> check_daily_estimation_task
            >> query_load_daily_estimation_results
        )

    return group
