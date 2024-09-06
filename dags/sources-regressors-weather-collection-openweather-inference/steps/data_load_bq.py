from airflow.models import DAG, TaskInstance
from common.operators.bigquery import OlvinBigQueryOperator


def register(start: TaskInstance, dag: DAG) -> TaskInstance:
    """Register tasks on the dag.

    Args:
        start (airflow.models.TaskInstance): Task instance all tasks will
            registered downstream from.
        dag (airflow.models.DAG): DAG instance to register tasks on.

    Returns:
        airflow.models.TaskInstance: The last task node in this section.
    """
    # Insert inference data in BigQuery
    load_hourly_inference_weather = OlvinBigQueryOperator(
        task_id="load_hourly_inference_weather",
        query="{% include './include/bigquery/load_hourly_inference_weather.sql' %}",
    )

    # Update regressors with new residual estimations
    query_predict_regressor = OlvinBigQueryOperator(
        task_id="query_predict_regressor",
        query="{% include './include/bigquery/estimated_regressor.sql' %}",
        billing_tier="med",

    )

    start >> load_hourly_inference_weather >> query_predict_regressor

    return query_predict_regressor
