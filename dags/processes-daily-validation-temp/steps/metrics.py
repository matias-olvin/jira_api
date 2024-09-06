from airflow.models import TaskInstance
from common.operators.bigquery import OlvinBigQueryOperator, SNSBigQueryOperator


def register(*, start: TaskInstance) -> TaskInstance:
    metrics = SNSBigQueryOperator(
        task_id="metrics",
        query="{% include './include/bigquery/metrics.sql' %}",
    )
    migrate = OlvinBigQueryOperator(
        task_id="migrate",
        query="{% include './include/bigquery/migrate.sql' %}",
    )
    start >> metrics >> migrate

    return migrate
