from __future__ import annotations

from airflow.models import DAG, TaskInstance
from common.operators.bigquery import OlvinBigQueryOperator
from common.operators.exceptions import MarkSuccessOperator
from common.utils.callbacks import task_end_custom_slack_alert


def register(start: TaskInstance, dag: DAG, env: str) -> TaskInstance:

    check_training_data_ingestion = MarkSuccessOperator(
        task_id="check_training_data_ingestion",
        on_failure_callback=task_end_custom_slack_alert(
            "U05FN3F961X",
            "U05M60N8DMX",
            channel=env,
            msg_title="Check Google Sheet Training Data Ingestion",
        ),
        dag=dag,
    )

    create_static_version_of_training_data = OlvinBigQueryOperator(
        task_id="create_static_version_of_training_data",
        query="{% include './include/bigquery/ingestion_alert/create_static_version_of_training_data.sql' %}",
    )

    start >> check_training_data_ingestion >> create_static_version_of_training_data

    return create_static_version_of_training_data
