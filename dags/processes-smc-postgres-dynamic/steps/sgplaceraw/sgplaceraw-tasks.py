from __future__ import annotations

from airflow.models import TaskInstance
from airflow.utils.task_group import TaskGroup
from common.operators.bigquery import OlvinBigQueryOperator


def register(start: TaskInstance) -> TaskGroup:
    with TaskGroup(group_id="SGPlaceRaw-tasks") as group:
        create = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",   
            task_id="SGPlaceRaw-create",
            query="{% include './include/bigquery/sgplaceraw/SGPlaceRaw-create.sql' %}",
        )
        filter_brands = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="SGPlaceRaw-filter-brands",
            query="{% include './include/bigquery/sgplaceraw/SGPlaceRaw-filter-brands.sql' %}",
        )
        area_analysis = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",      
            task_id="SGPlaceRaw-area-analysis",
            query="{% include './include/bigquery/sgplaceraw/SGPlaceRaw-area-analysis.sql' %}",
        )
        malls_base_create = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="malls-base-create",
            query="{% include './include/bigquery/sgplaceraw/malls-base-create.sql' %}",
        )
        include_fksgcenters = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="SGPlaceRaw-include-fksgcenters",
            query="{% include './include/bigquery/sgplaceraw/SGPlaceRaw-include-fksgcenters.sql' %}",
        )
        update_wrong_coords = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="SGPlaceRaw-update-wrong-coords",
            query="{% include './include/bigquery/sgplaceraw/SGPlaceRaw-update-wrong-coords.sql' %}",
        )
        
        (
            start 
            >> create
            >> filter_brands
            >> area_analysis
            >> malls_base_create
            >> include_fksgcenters
            >> update_wrong_coords
        )
    
    return group