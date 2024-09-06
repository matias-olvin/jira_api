from airflow.utils.task_group import TaskGroup
from airflow.models import TaskInstance, DAG
from common.operators.bigquery import OlvinBigQueryOperator


def register(start: TaskInstance, dag: DAG) -> TaskGroup:
    """Register tasks on the dag.

    Args:
        dag (airflow.models.DAG): DAG instance to register tasks on.
        start (airflow.models.TaskInstance): Task instance all tasks will
            registered downstream from.

    Returns:
        airflow.models.TaskInstance: The last task node in this section.
    """
    with TaskGroup(group_id="classifier_prior_distances_task_group") as group:
        query_filtering_places = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="query_filtering_places",
            query="{% include './include/bigquery/visits_share/classifier/filtering_places.sql' %}",
        )

        query_training_data_raw = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="query_training_data_raw",
            query="{% include './include/bigquery/visits_share/classifier/training_data_raw.sql' %}",
            billing_tier="higher",
        )

        query_model_input = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="query_model_input",
            query="{% include './include/bigquery/visits_share/classifier/model_input.sql' %}",
        )

        query_prior_distances = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="query_prior_distances",
            query="{% include './include/bigquery/visits_share/prior_distances/distances_to_centroid.sql' %}",
            billing_tier="high",
        )

        (
            start
            >> query_filtering_places
            >> query_training_data_raw
            >> query_model_input
            >> query_prior_distances
        )

    return group
