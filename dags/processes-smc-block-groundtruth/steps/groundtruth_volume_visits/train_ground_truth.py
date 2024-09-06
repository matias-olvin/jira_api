import pendulum
from airflow.models import DAG, TaskInstance
from airflow.operators.bash_operator import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from common.operators.bigquery import OlvinBigQueryOperator
from common.operators.exceptions import MarkSuccessOperator
from common.utils import callbacks, slack_users


def register(start: TaskInstance, dag: DAG) -> TaskGroup:
    """Register tasks on the dag.

    Args:
        dag (airflow.models.DAG): DAG instance to register tasks on.
        start (airflow.models.TaskInstance): Task instance all tasks will
            registered downstream from.

    Returns:
        airflow.models.TaskInstance: The last task node in this section.
    """
    with TaskGroup(group_id="train_ground_truth_task_group") as group:

        create_gtvm_output_backup = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="create_gtvm_output_backup",
            query="{% include './include/bigquery/groundtruth_volume_visits/create_gtvm_output_backup.sql' %}",
        )

        start >> create_gtvm_output_backup

        trigger_task = BashOperator(
            task_id="trigger_gtvm_training",
            bash_command=f"gcloud composer environments run prod-sensormatic "
            "--project {{ params['sns_project'] }} "
            "--location europe-west1 "
            "--impersonate-service-account {{ params['cross_project_service_account'] }} "
            f'dags trigger -- -e "{{{{ ds }}}} {pendulum.now().time()}" smc_gtvm_training '
            "|| true ",  # this line is needed due to gcloud bug.
        )

        create_gtvm_output_backup >> trigger_task

        mark_success_to_complete_gtvm_training = MarkSuccessOperator(
            task_id="mark_success_to_complete_gtvm_training",
        )

        send_gtvm_output_smc = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="send_gtvm_output_smc",
            query="{% include './include/bigquery/groundtruth_volume_visits/send_gtvm_output_smc.sql' %}",
        )

        trigger_task >> mark_success_to_complete_gtvm_training >> send_gtvm_output_smc

        monitoring_create_factor_per_poi = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="monitoring_create_factor_per_poi",
            query="{% include './include/bigquery/groundtruth_volume_visits/tests/factor_per_poi.sql' %}",
        )

        monitoring_check_factor_per_poi = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="monitoring_check_factor_per_poi",
            query="{% include './include/bigquery/groundtruth_volume_visits/tests/monitoring_check_factor_per_poi.sql' %}",
            retries=1,
            on_failure_callback=callbacks.task_fail_slack_alert(
                slack_users.MATIAS,
            ),
            on_retry_callback=callbacks.task_retry_slack_alert(
                slack_users.MATIAS,
            ),
        )

        query_generate_factor_per_poi = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="query_generate_factor_per_poi",
            query="{% include './include/bigquery/groundtruth_volume_visits/generate_factor_per_poi.sql' %}",
        )

        (
            send_gtvm_output_smc
            >> query_generate_factor_per_poi
            >> monitoring_create_factor_per_poi
            >> monitoring_check_factor_per_poi
        )

        groundtruth_end = EmptyOperator(
            task_id="groundtruth_end",
            on_success_callback=callbacks.task_end_custom_slack_alert(
                slack_users.MATIAS, msg_title="*Groundtruth* model created."
            ),
        )
        monitoring_check_factor_per_poi >> groundtruth_end

    return group
