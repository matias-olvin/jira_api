from __future__ import annotations

from airflow.models import TaskInstance
from airflow.utils.task_group import TaskGroup

from common.operators.bigquery import OlvinBigQueryOperator


def register(start: TaskInstance) -> TaskGroup:
    with TaskGroup(group_id="SGBrandRaw-tests") as group:
        test_duplicates = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="SGBrandRaw-test-duplicates",
            query="{% include './include/bigquery/sgbrandraw/tests/test-duplicates.sql' %}",
        )
        test_nulls = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="SGBrandRaw-test-nulls",
            query="{% include './include/bigquery/sgbrandraw/tests/test-nulls.sql' %}",
        )
        test_sensitive_naics = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="SGBrandRaw-test-sensitive-naics",
            query="{% include './include/bigquery/sgbrandraw/tests/test-sensitive-naics.sql' %}",
        )
        test_invalid_domain_format= OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="SGBrandRaw-test-invalid-domain-format",
            query="{% include './include/bigquery/sgbrandraw/tests/test-invalid-domain-format.sql' %}",
        )
        
        start >> [test_duplicates, test_nulls, test_sensitive_naics, test_invalid_domain_format]
    
    return group