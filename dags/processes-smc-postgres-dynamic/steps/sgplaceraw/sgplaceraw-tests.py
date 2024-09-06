from __future__ import annotations

from airflow.models import TaskInstance
from airflow.utils.task_group import TaskGroup

from common.operators.bigquery import OlvinBigQueryOperator


def register(start: TaskInstance) -> TaskGroup:
    with TaskGroup(group_id="SGPlaceRaw-tests") as group:
        test_duplicates = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="SGPlaceRaw-test-duplicates",
            query="{% include './include/bigquery/sgplaceraw/tests/test-duplicates.sql' %}",
        )
        test_nulls = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="SGPlaceRaw-test-nulls",
            query="{% include './include/bigquery/sgplaceraw/tests/test-nulls.sql' %}",
        )
        test_sensitive_naics = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="SGPlaceRaw-test-sensitive-naics",
            query="{% include './include/bigquery/sgplaceraw/tests/test-sensitive-naics.sql' %}",
        )
        
        start >> [test_duplicates, test_nulls, test_sensitive_naics]
    
    return group