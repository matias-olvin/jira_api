from airflow.models import TaskInstance
from common.operators.bigquery import OlvinBigQueryOperator


def register(start: TaskInstance) -> TaskInstance:

    create_snapshot_of_prior_brand_visits = OlvinBigQueryOperator(
        task_id="create_snapshot_of_prior_brand_visits",
        query="{% include './include/bigquery/snapshots/create_snapshot_of_prior_brand_visits.sql' %}",
    )

    copy_prior_brand_visits = OlvinBigQueryOperator(
        task_id="copy_prior_brand_visits",
        query="{% include './include/bigquery/missing_brands_categories_tables_1/copy_prior_brand_visits.sql' %}",
    )

    create_missing_brands = OlvinBigQueryOperator(
        task_id="create_missing_brands",
        query="{% include './include/bigquery/missing_brands_categories_tables_1/create_missing_brands.sql' %}",
    )

    create_missing_categories = OlvinBigQueryOperator(
        task_id="create_missing_categories",
        query="{% include './include/bigquery/missing_brands_categories_tables_1/create_missing_categories.sql' %}",
    )

    (
        start
        >> create_snapshot_of_prior_brand_visits
        >> copy_prior_brand_visits
        >> create_missing_brands
        >> create_missing_categories
    )

    return create_missing_categories
