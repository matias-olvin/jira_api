from __future__ import annotations

from datetime import datetime

import numpy as np
from airflow.models import TaskInstance, Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
from common.operators.bigquery import OlvinBigQueryOperator
from common.operators.exceptions import MarkSuccessOperator
from common.utils import slack_users
from common.utils.callbacks import task_end_custom_slack_alert
from common.utils.data_feeds import get_backfill_date
from dateutil.relativedelta import relativedelta


def register(dag, start: TaskInstance) -> TaskGroup:
    """
    Register tasks on the dag

    Args:
        start (airflow.models.TaskInstance): Task instance all tasks will
        be registered downstream from.

    Returns:
        airflow.utils.task_group.TaskGroup: The task group in this section.
    """

    def branch_task_first_monday_fn(**context):
        """
        If the run date is the first Monday of the month, then return the task ID of the task that
        clears the historical table. Otherwise, return the task ID of the task that clears the weekly
        table.
        :return: The name of the task to be executed.
        """
        run_date = context["next_ds"]
        first_monday = Variable.get("first_monday")
        return "dewey.branch_task_insert" if run_date == first_monday else "dewey.end"

    def branch_task_insert_fn(**context):
        smc_end_date_prev = Variable.get("smc_end_date_prev")
        backfill_date = get_backfill_date(smc_end_date_prev)

        run_date = str(context["next_ds"])

        run_date = datetime.strptime(run_date, "%Y-%m-%d")

        print(f"run_date: {run_date}")
        print(f"backfill_date: {backfill_date}")

        if run_date == backfill_date:
            return "dewey.check_backfill_before_continuing"
        else:
            return "dewey.check_monthly_increment_before_continuing"

    with TaskGroup(group_id="dewey") as group:

        end = EmptyOperator(task_id="end", trigger_rule="all_done")

        sensor = ExternalTaskSensor(
            task_id="placekey_sensor",
            external_dag_id="{{ params['placekeys_feed'] }}",
            external_task_id="end",
            execution_date_fn=lambda dt: dt.replace(
                day=1,
                hour=6,
            )
            - relativedelta(months=1),
            mode="reschedule",
            poke_interval=60 * 5,
            timeout=60 * 60 * 24,
        )
        start >> sensor

        branch_task_first_monday = BranchPythonOperator(
            task_id="branch_task_first_monday",
            provide_context=True,
            python_callable=branch_task_first_monday_fn,
        )

        sensor >> branch_task_first_monday

        branch_task_insert = BranchPythonOperator(
            task_id="branch_task_insert",
            provide_context=True,
            python_callable=branch_task_insert_fn,
        )

        check_backfill_before_continuing = MarkSuccessOperator(
            task_id="check_backfill_before_continuing",
            on_failure_callback=task_end_custom_slack_alert(
                slack_users.MATIAS,
                channel="prod",
                msg_title="Confirm path before continuing",
            ),
        )

        clear_gcs_placekeys_monthly = GCSDeleteObjectsOperator(
            task_id="clear_gcs_placekeys_monthly",
            bucket_name="{{ params['dewey_bucket'] }}",
            prefix="{{ params['placekeys_table'] }}/",
        )

        clear_gcs_store_visits_monthly_backfill = GCSDeleteObjectsOperator(
            task_id="clear_gcs_store_visits_monthly_backfill",
            bucket_name="{{ params['dewey_bucket'] }}",
            prefix="{{ params['store_visits_table'].replace('_','-') }}/",
        )

        clear_gcs_store_visitors_monthly_backfill = GCSDeleteObjectsOperator(
            task_id="clear_gcs_store_visitors_monthly_backfill",
            bucket_name="{{ params['dewey_bucket'] }}",
            prefix="{{ params['store_visitors_table'].replace('_','-') }}/",
        )

        clear_gcs_store_visits_trend_monthly_backfill = GCSDeleteObjectsOperator(
            task_id="clear_gcs_store_visits_trend_monthly_backfill",
            bucket_name="{{ params['dewey_bucket'] }}",
            prefix="{{ params['store_visits_trend_table'].replace('_','-') }}/",
        )

        check_monthly_increment_before_continuing = MarkSuccessOperator(
            task_id="check_monthly_increment_before_continuing",
            on_failure_callback=task_end_custom_slack_alert(
                slack_users.MATIAS,
                channel="prod",
                msg_title="Confirm path before continuing",
            ),
        )

        clear_gcs_store_visits_monthly_increment = GCSDeleteObjectsOperator(
            task_id="clear_gcs_store_visits_monthly_increment",
            bucket_name="{{ params['dewey_bucket'] }}",
            prefix="{{ params['store_visits_table'].replace('_','-') }}/{{ next_ds.replace('-', '/') }}/",
        )

        clear_gcs_store_visitors_monthly_increment = GCSDeleteObjectsOperator(
            task_id="clear_gcs_store_visitors_monthly_increment",
            bucket_name="{{ params['dewey_bucket'] }}",
            prefix="{{ params['store_visitors_table'].replace('_','-') }}/{{ next_ds.replace('-', '/') }}/",
        )

        clear_gcs_store_visits_trend_monthly_increment = GCSDeleteObjectsOperator(
            task_id="clear_gcs_store_visits_trend_monthly_increment",
            bucket_name="{{ params['dewey_bucket'] }}",
            prefix="{{ params['store_visits_trend_table'].replace('_','-') }}/{{ next_ds.replace('-', '/') }}/",
        )

        export_placekeys_monthly = OlvinBigQueryOperator(
            task_id="export_placekeys_monthly",
            query="{% include './bigquery/dewey/placekeys_monthly.sql' %}",
        )

        export_store_visits_monthly_backfill = OlvinBigQueryOperator(
            task_id="export_store_visits_monthly_backfill",
            query="{% include './bigquery/dewey/store_visits_monthly_backfill.sql' %}",
            billing_tier="high",
        )

        export_store_visitors_monthly_backfill = OlvinBigQueryOperator(
            task_id="export_store_visitors_monthly_backfill",
            query="{% include './bigquery/dewey/store_visitors_monthly_backfill.sql' %}",
            billing_tier="high",
        )

        export_store_visits_trend_monthly_backfill = OlvinBigQueryOperator(
            task_id="export_store_visits_trend_monthly_backfill",
            query="{% include './bigquery/dewey/store_visits_trend_monthly_backfill.sql' %}",
        )

        export_store_visits_monthly_increment = OlvinBigQueryOperator(
            task_id="export_store_visits_monthly_increment",
            query="{% include './bigquery/dewey/store_visits_monthly_increment.sql' %}",
            billing_tier="med",
        )

        export_store_visitors_monthly_increment = OlvinBigQueryOperator(
            task_id="export_store_visitors_monthly_increment",
            query="{% include './bigquery/dewey/store_visitors_monthly_increment.sql' %}",
        )

        export_store_visits_trend_monthly_increment = OlvinBigQueryOperator(
            task_id="export_store_visits_trend_monthly_increment",
            query="{% include './bigquery/dewey/store_visits_trend_monthly_increment.sql' %}",
        )

        branch_task_first_monday >> branch_task_insert
        branch_task_first_monday >> end

        (
            branch_task_insert
            >> check_backfill_before_continuing
            >> clear_gcs_placekeys_monthly
            >> export_placekeys_monthly
            >> clear_gcs_store_visits_monthly_backfill
            >> clear_gcs_store_visitors_monthly_backfill
            >> clear_gcs_store_visits_trend_monthly_backfill
            >> [
                export_store_visits_monthly_backfill,
                export_store_visitors_monthly_backfill,
                export_store_visits_trend_monthly_backfill,
            ]
            >> end
        )
        (
            branch_task_insert
            >> check_monthly_increment_before_continuing
            >> clear_gcs_store_visits_monthly_increment
            >> clear_gcs_store_visitors_monthly_increment
            >> clear_gcs_store_visits_trend_monthly_increment
            >> [
                export_store_visits_monthly_increment,
                export_store_visitors_monthly_increment,
                export_store_visits_trend_monthly_increment,
            ]
            >> end
        )

    return group
