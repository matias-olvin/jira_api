from airflow.models import TaskInstance
from airflow.utils.task_group import TaskGroup
from common.operators.bigquery import OlvinBigQueryOperator


def register(start: TaskInstance) -> TaskInstance:
    with TaskGroup(group_id="cameo_demo_shopping_agg_group") as group:
        create_cameo_monthly = OlvinBigQueryOperator(
            task_id="create_cameo_monthly",
            query="{% include './include/bigquery/cameo_demo_shopping/create_cameo_monthly.sql' %}",
        )

        create_cross_shopping = OlvinBigQueryOperator(
            task_id="create_cross_shopping",
            query="{% include './include/bigquery/cameo_demo_shopping/create_cross_shopping.sql' %}",
        )

        create_cameo = OlvinBigQueryOperator(
            task_id="create_cameo",
            query="{% include './include/bigquery/cameo_demo_shopping/create_cameo.sql' %}",
        )

        cross_shopping_perc = OlvinBigQueryOperator(
            task_id="cross_shopping_perc",
            query="{% include './include/bigquery/cameo_demo_shopping/cross_shopping_perc.sql' %}",
        )

        start >> [
            create_cameo,
            create_cross_shopping,
            cross_shopping_perc,
        ]

        create_cameo >> create_cameo_monthly

    return group
