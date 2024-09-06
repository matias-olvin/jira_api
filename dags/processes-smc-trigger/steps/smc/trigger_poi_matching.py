import pendulum
from airflow.models import DAG, TaskInstance
from airflow.operators.bash_operator import BashOperator
from airflow.utils.task_group import TaskGroup
from common.operators.bigquery import OlvinBigQueryOperator
from common.operators.exceptions import MarkSuccessOperator


def register(start: TaskInstance, dag: DAG) -> TaskInstance:
    """
    Register tasks on the dag

    Args:
        dag (airflow.models.DAG): DAG instance to register tasks on.
        start (airflow.models.TaskInstance): Task instance all tasks will
        be registered downstream from.

    Returns:
        airflow.models.TaskInstance: The last task node in this section.
    """
    with TaskGroup(group_id="trigger_poi_matching") as group:

        create_places_dynamic_placekey = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="create_places_dynamic_placekey",
            query="{% include './include/bigquery/poi_matching/join_placekey_to_places_dynamic.sql' %}",
        )

        check_placekey_pattern = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="check_placekey_pattern",
            query="{% include './include/bigquery/poi_matching/check_placekey_pattern.sql' %}",
        )

        trigger = BashOperator(
            task_id="trigger_poi_matching",
            bash_command="gcloud composer environments run {{ params['sns_composer'] }} "
            "--project {{ params['sns_project'] }} "
            "--location {{ params['sns_composer_location'] }} "
            "--impersonate-service-account {{ params['cross_project_service_account'] }} "
            f'dags trigger -- -e "{{{{ ds }}}} {pendulum.now().time()}" poi_matching '
            "|| true ",  # this line is needed due to gcloud bug.
        )

        mark_success_to_complete_poi_matching = MarkSuccessOperator(
            task_id="mark_success_to_complete_poi_matching"
        )

        (
            start
            >> create_places_dynamic_placekey
            >> check_placekey_pattern
            >> trigger
            >> mark_success_to_complete_poi_matching
        )

    return group
