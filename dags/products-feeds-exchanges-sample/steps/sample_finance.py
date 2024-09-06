from __future__ import annotations

from airflow.models import TaskInstance
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
from common.operators.bigquery import OlvinBigQueryOperator


def register(start: TaskInstance) -> TaskGroup:
    """
    Register tasks on the dag

    Args:
        start (airflow.models.TaskInstance): Task instance all tasks will
        be registered downstream from.

    Returns:
        airflow.utils.task_group.TaskGroup: The task group in this section.
    """

    with TaskGroup(group_id="sample-finance") as group:

        sensor = ExternalTaskSensor(
            task_id="placekey_sensor",
            external_dag_id="{{ params['placekeys_feed'] }}",
            external_task_id="end",
            execution_date_fn=lambda dt: dt.replace(
                day=1,
                hour=6,
            ),
            mode="reschedule",
            poke_interval=60 * 5,
            timeout=60 * 60 * 24,
        )

        clear_gcs_placekeys = GCSDeleteObjectsOperator(
            task_id="clear_gcs_placekeys",
            bucket_name="{{ params['sample_bucket'] }}",
            prefix="finance/{{ params['placekeys_table'] }}/",
        )

        clear_gcs_store_visits = GCSDeleteObjectsOperator(
            task_id="clear_gcs_store_visits",
            bucket_name="{{ params['sample_bucket'] }}",
            prefix="finance/{{ params['store_visits_table'].replace('_','-') }}/",
        )

        clear_gcs_store_visitors = GCSDeleteObjectsOperator(
            task_id="clear_gcs_store_visitors",
            bucket_name="{{ params['sample_bucket'] }}",
            prefix="finance/{{ params['store_visitors_table'].replace('_','-') }}/",
        )

        clear_gcs_store_visits_trend = GCSDeleteObjectsOperator(
            task_id="clear_gcs_store_visits_trend",
            bucket_name="{{ params['sample_bucket'] }}",
            prefix="finance/{{ params['store_visits_trend_table'].replace('_','-') }}/",
        )

        clear_gcs_store_visits_daily = GCSDeleteObjectsOperator(
            task_id="clear_gcs_store_visits_daily",
            bucket_name="{{ params['sample_bucket'] }}",
            prefix="finance/{{ params['store_visits_daily_table'].replace('_','-') }}/",
        )

        export_placekeys = OlvinBigQueryOperator(
            task_id="export_placekeys",
            query="{% include './bigquery/finance/placekeys.sql' %}",
        )

        export_store_visits = OlvinBigQueryOperator(
            task_id="export_store_visits",
            query="{% include './bigquery/finance/store_visits.sql' %}",
            billing_tier="high",
        )

        export_store_visitors = OlvinBigQueryOperator(
            task_id="export_store_visitors",
            query="{% include './bigquery/finance/store_visitors.sql' %}",
            billing_tier="high",
        )

        export_store_visits_trend = OlvinBigQueryOperator(
            task_id="export_store_visits_trend",
            query="{% include './bigquery/finance/store_visits_trend.sql' %}",
            billing_tier="high",
        )

        export_store_visits_daily = OlvinBigQueryOperator(
            task_id="export_store_visits_daily",
            query="{% include './bigquery/finance/store_visits_daily.sql' %}",
            billing_tier="high",
        )

        (
            start
            >> sensor
            >> clear_gcs_placekeys
            >> [
                clear_gcs_store_visits,
                clear_gcs_store_visitors,
                clear_gcs_store_visits_trend,
                clear_gcs_store_visits_daily,
            ]
            >> export_placekeys
            >> [
                export_store_visits,
                export_store_visitors,
                export_store_visits_trend,
                export_store_visits_daily,
            ]
        )

    return group
