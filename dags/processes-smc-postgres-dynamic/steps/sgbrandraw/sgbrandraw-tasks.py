from __future__ import annotations

from airflow.models import TaskInstance
from airflow.utils.task_group import TaskGroup
from common.operators.bigquery import OlvinBigQueryOperator


def register(start: TaskInstance) -> TaskGroup:
    with TaskGroup(group_id="SGBrandRaw-tasks") as group:
        create = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",   
            task_id="SGBrandRaw-create",
            query="{% include './include/bigquery/sgbrandraw/SGBrandRaw-create.sql' %}",
        )
        filter_brands = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="SGBrandRaw-filter-brands",
            query="{% include './include/bigquery/sgbrandraw/SGBrandRaw-filter-brands.sql' %}",
        )
        obtain_domains = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="SGBrandRaw-obtain-domains",
            query="{% include './include/bigquery/sgbrandraw/SGBrandRaw-obtain-domains.sql' %}",
        )

        start >> create >> filter_brands >> obtain_domains
    
    return group