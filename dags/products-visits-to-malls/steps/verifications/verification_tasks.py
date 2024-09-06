from __future__ import annotations

from airflow.models import TaskInstance
from common.operators.bigquery import OlvinBigQueryOperator


def register(start: TaskInstance) -> TaskInstance:

    malls_to_remove_verification = OlvinBigQueryOperator(
        task_id="malls_to_remove_verifications",
        query="{% include './include/bigquery/verifications/combined_verifications.sql' %}",
    )

    start >> malls_to_remove_verification

    return malls_to_remove_verification
