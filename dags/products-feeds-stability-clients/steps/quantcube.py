from __future__ import annotations

from datetime import datetime

from airflow.models import DAG, TaskInstance, Variable
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.utils.task_group import TaskGroup
from common.operators.bigquery import OlvinBigQueryOperator
from common.utils.data_feeds import get_backfill_date


def register(start: TaskInstance, dag: DAG) -> TaskGroup:
    """
    Register tasks on the dag

    Args:
        start (airflow.models.TaskInstance): Task instance all tasks will
        be registered downstream from.
        dag (airflow.models.DAG): The dag object.

    Returns:
        airflow.utils.task_group.TaskGroup: The task group in this section.
    """

    def branch_task_insert_fn(**context):
        smc_end_date_prev = Variable.get("smc_end_date_prev")
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
                return "quantcube.clear_gcs_placekeys_monthly"  # backfill
            else:
                return "quantcube.clear_gcs_store_visitors_monthly_increment"  # increment
        else:
            return "quantcube.end"  # end

    with TaskGroup(group_id="quantcube") as group:

        DATAPROC_CLUSTER_NAME = "pby-product-p-dpc-euwe1-quantcube-s3"

        branch_task_insert = BranchPythonOperator(
            task_id="branch_task_insert",
            provide_context=True,
            python_callable=branch_task_insert_fn,
        )

        start >> branch_task_insert

        clear_gcs_placekeys_monthly = GCSDeleteObjectsOperator(
            task_id="clear_gcs_placekeys_monthly",
            bucket_name="{{ params['feeds_staging_gcs_bucket'] }}",
            prefix="quantcube/export_date={{ next_ds.replace('-', '') }}/{{ params['placekeys_table'] }}/",
        )

        clear_gcs_store_visitors_monthly_backfill = GCSDeleteObjectsOperator(
            task_id="clear_gcs_store_visitors_monthly_backfill",
            bucket_name="{{ params['feeds_staging_gcs_bucket'] }}",
            prefix="quantcube/export_date={{ next_ds.replace('-', '') }}/{{ params['store_visitors_table'].replace('_','-') }}/",
        )

        clear_gcs_store_visits_trend_monthly_backfill = GCSDeleteObjectsOperator(
            task_id="clear_gcs_store_visits_trend_monthly_backfill",
            bucket_name="{{ params['feeds_staging_gcs_bucket'] }}",
            prefix="quantcube/export_date={{ next_ds.replace('-', '') }}/{{ params['store_visits_trend_table'].replace('_','-') }}/",
        )

        clear_gcs_store_visitors_monthly_increment = GCSDeleteObjectsOperator(
            task_id="clear_gcs_store_visitors_monthly_increment",
            bucket_name="{{ params['feeds_staging_gcs_bucket'] }}",
            prefix="quantcube/export_date={{ next_ds.replace('-', '') }}/{{ params['store_visitors_table'].replace('_','-') }}/",
        )

        clear_gcs_store_visits_trend_monthly_increment = GCSDeleteObjectsOperator(
            task_id="clear_gcs_store_visits_trend_monthly_increment",
            bucket_name="{{ params['feeds_staging_gcs_bucket'] }}",
            prefix="quantcube/export_date={{ next_ds.replace('-', '') }}/{{ params['store_visits_trend_table'].replace('_','-') }}/",
        )

        export_placekeys_monthly = OlvinBigQueryOperator(
            task_id="export_placekeys_monthly",
            query="{% include './bigquery/quantcube/placekeys_monthly.sql' %}",
        )

        export_store_visitors_monthly_backfill = OlvinBigQueryOperator(
            task_id="export_store_visitors_monthly_backfill",
            query="{% include './bigquery/quantcube/store_visitors_monthly_backfill.sql' %}",
            billing_tier="high",
        )

        export_store_visits_trend_monthly_backfill = OlvinBigQueryOperator(
            task_id="export_store_visits_trend_monthly_backfill",
            query="{% include './bigquery/quantcube/store_visits_trend_monthly_backfill.sql' %}",
        )

        export_store_visitors_monthly_increment = OlvinBigQueryOperator(
            task_id="export_store_visitors_monthly_increment",
            query="{% include './bigquery/quantcube/store_visitors_monthly_increment.sql' %}",
            billing_tier="high",
        )

        export_store_visits_trend_monthly_increment = OlvinBigQueryOperator(
            task_id="export_store_visits_trend_monthly_increment",
            query="{% include './bigquery/quantcube/store_visits_trend_monthly_increment.sql' %}",
        )

        source = "gs://{{ params['feeds_staging_gcs_bucket'] }}/quantcube/export_date={{ next_ds.replace('-', '') }}"
        destination = "s3a://{{ params['quantcube_bucket'] }}/poi_data/passby_live/export_date={{ next_ds.replace('-', '') }}"

        create_export_to_s3_cluster = BashOperator(
            task_id="create_export_to_s3_cluster",
            bash_command=f"gcloud dataproc clusters create {DATAPROC_CLUSTER_NAME} "
            f"--region {dag.params['dataproc_region']} --master-machine-type n2-standard-4 --master-boot-disk-size 500 "
            "--num-workers 4 --worker-machine-type n2-standard-8 --worker-boot-disk-size 500 "
            "--image-version 1.5-debian10 "
            f"--initialization-actions 'gs://{dag.params['feeds_keys_gcs_bucket']}/pby-aws-send-quantcube.sh' "
            f"--project {{{{ var.value.env_project }}}} "
            "--max-age 10h --max-idle 15m",
            trigger_rule="none_failed_min_one_success",
        )

        submit_export_job = BashOperator(
            task_id="submit_export_job",
            bash_command=f"gcloud dataproc jobs submit hadoop  "
            f"--project={{{{ var.value.env_project }}}} "
            f"--region={dag.params['dataproc_region']} --cluster={DATAPROC_CLUSTER_NAME} "
            "--jar file:///usr/lib/hadoop-mapreduce/hadoop-distcp.jar "
            f"-- -strategy dynamic -bandwidth 1000 -update {source} {destination}",
        )

        end = EmptyOperator(task_id="end", trigger_rule="none_failed")

        (branch_task_insert >> end)

        (
            branch_task_insert
            >> clear_gcs_placekeys_monthly
            >> export_placekeys_monthly
            >> clear_gcs_store_visitors_monthly_backfill
            >> clear_gcs_store_visits_trend_monthly_backfill
            >> [
                export_store_visitors_monthly_backfill,
                export_store_visits_trend_monthly_backfill,
            ]
            >> create_export_to_s3_cluster
            >> submit_export_job
            >> end
        )

        (
            branch_task_insert
            >> clear_gcs_store_visitors_monthly_increment
            >> clear_gcs_store_visits_trend_monthly_increment
            >> [
                export_store_visitors_monthly_increment,
                export_store_visits_trend_monthly_increment,
            ]
            >> create_export_to_s3_cluster
            >> submit_export_job
            >> end
        )

    return group
