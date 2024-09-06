from airflow.models import TaskInstance
from common.operators.bigquery import OlvinBigQueryOperator


def register(start: TaskInstance) -> TaskInstance:

    create_sgplaceraw_in_manually_add_pois = OlvinBigQueryOperator(
        task_id="create_sgplaceraw_in_manually_add_pois",
        query="{% include './include/bigquery/create_sgplaceraw_in_manually_add_pois.sql' %}",
    )

    start >> create_sgplaceraw_in_manually_add_pois

    return create_sgplaceraw_in_manually_add_pois
