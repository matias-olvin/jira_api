from __future__ import annotations

from datetime import datetime

import numpy as np
from airflow.models import DAG, TaskInstance, Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
from common.operators.bigquery import OlvinBigQueryOperator
from common.utils.data_feeds import get_backfill_date
from dateutil.relativedelta import relativedelta


def register(start: TaskInstance, dag: DAG) -> TaskGroup:
    """
    Register tasks on the dag

    Args:
        start (airflow.models.TaskInstance): Task instance all tasks will
        be registered downstream from.
        dag (airflow.models.DAG): The dag to register tasks on.

    Returns:
        airflow.utils.task_group.TaskGroup: The task group in this section.
    """

    def set_first_monday(**context) -> None:
        """
        The function takes the logical date of the dag run and uses the numpy busday_offset function
        to find the first Monday of the month
        """
        run_date = context["data_interval_end"]
        first_monday = f"{np.busday_offset(run_date.strftime('%Y-%m'), 0, roll='forward', weekmask='Mon')}"
        Variable.set("first_monday", first_monday)

    def branch_task_check_backfill_fn(**context):
        smc_end_date_prev = Variable.get("smc_end_date_prev")  # CHECK DATA TYPES HERE
        backfill_date = get_backfill_date(smc_end_date_prev)
        first_monday = Variable.get("first_monday")

        run_date = str(context["next_ds"])

        print(f"run_date: {run_date}")
        print(f"backfill_date: {backfill_date}")
        print(f"first_monday: {first_monday}")

        if isinstance(run_date, str):
            run_date = datetime.strptime(run_date, "%Y-%m-%d")
        if isinstance(backfill_date, str):
            backfill_date = datetime.strptime(backfill_date, "%Y-%m-%d")
        if isinstance(first_monday, str):
            first_monday = datetime.strptime(first_monday, "%Y-%m-%d")

        print(f"run_date == first_monday: {run_date.date() == first_monday.date()}")
        print(f"run_date == backfill_date: {run_date.date() == backfill_date.date()}")

        if run_date.date() == first_monday.date():
            if run_date.date() == backfill_date.date():
                return "common.clear_gcs_backfill"
            else:
                return "common.clear_gcs"
        else:
            return "common.clear_gcs"

    with TaskGroup(group_id="common") as group:

        sensor = ExternalTaskSensor(
            task_id="placekey_sensor",
            external_dag_id="{{ params['placekeys_feed'] }}",
            external_task_id="end",
            execution_date_fn=lambda dt: (dt + relativedelta(days=1)).replace(day=1, hour=6) - relativedelta(months=1),
            mode="reschedule",
            poke_interval=60 * 5,
            timeout=60 * 60 * 24 * 3, # 3 days timeout
        )

        push_first_monday = PythonOperator(
            task_id="push_first_monday",
            python_callable=set_first_monday,
        )

        branch_task_check_backfill = BranchPythonOperator(
            task_id="branch_task_check_backfill",
            provide_context=True,
            python_callable=branch_task_check_backfill_fn,
        )

        clear_gcs_backfill = GCSDeleteObjectsOperator(
            task_id="clear_gcs_backfill",
            bucket_name="{{ params['feeds_staging_gcs_daily_bucket'] }}",
            prefix="export_date={{ next_ds.replace('-', '') }}/type_2/",
        )

        export_daily_feed_backfill = OlvinBigQueryOperator(
            task_id="export_daily_feed_backfill",
            query="{% include './include/bigquery/backfill/export_daily_feed.sql' %}",
            billing_tier="high",
        )

        export_daily_feed_finance_backfill = OlvinBigQueryOperator(
            task_id="export_daily_feed_finance_backfill",
            query="{% include './include/bigquery/backfill/export_daily_feed_finance.sql' %}",
            billing_tier="high",
        )

        clear_gcs = GCSDeleteObjectsOperator(
            task_id="clear_gcs",
            bucket_name="{{ params['feeds_staging_gcs_daily_bucket'] }}",
            prefix="export_date={{ next_ds.replace('-', '') }}/type_2/",
        )

        export_daily_feed = OlvinBigQueryOperator(
            task_id="export_daily_feed",
            query="{% include './include/bigquery/daily/export_daily_feed.sql' %}",
            billing_tier="high",
        )

        export_daily_feed_finance = OlvinBigQueryOperator(
            task_id="export_daily_feed_finance",
            query="{% include './include/bigquery/daily/export_daily_feed_finance.sql' %}",
            billing_tier="high",
        )

        end_branching_task = EmptyOperator(
            task_id="end_branching_task", trigger_rule="none_failed_min_one_success"
        )

        start >> sensor >> push_first_monday >> branch_task_check_backfill

        (
            branch_task_check_backfill
            >> clear_gcs_backfill
            >> export_daily_feed_backfill
            >> export_daily_feed_finance_backfill
            >> end_branching_task
        )
        (
            branch_task_check_backfill
            >> clear_gcs
            >> export_daily_feed
            >> export_daily_feed_finance
            >> end_branching_task
        )

    return group
