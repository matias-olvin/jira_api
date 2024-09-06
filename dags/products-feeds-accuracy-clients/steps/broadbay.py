from __future__ import annotations

from airflow.models import DAG, TaskInstance, Variable
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.utils.task_group import TaskGroup
from common.operators.bigquery import OlvinBigQueryOperator
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator


def register(start: TaskInstance, dag: DAG, env: str) -> TaskGroup:
    """
    Register tasks on the dag

    Args:
        start (airflow.models.TaskInstance): Task instance all tasks will
        be registered downstream from.
        dag (airflow.models.DAG): The dag object.
        env (str): The environment the dag is running in.

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
        return (
            "broadbay.clear_gcs_placekeys_monthly"
            if run_date == first_monday
            else "broadbay.end"
        )

    dataproc_cluster_name = (
        "pby-product-d-dpc-euwe1-broadbay-s3"
        if env == "dev"
        else "pby-product-p-dpc-euwe1-broadbay-s3"
    )

    with TaskGroup(group_id="broadbay") as group:

        branch_task_first_monday = BranchPythonOperator(
            task_id="branch_task_first_monday",
            provide_context=True,
            python_callable=branch_task_first_monday_fn,
        )

        start >> branch_task_first_monday

        clear_gcs_placekeys_monthly = GCSDeleteObjectsOperator(
            task_id="clear_gcs_placekeys_monthly",
            bucket_name="{{ params['feeds_staging_gcs_no_pays_bucket'] }}",
            prefix="broadbay/export_date={{ next_ds.replace('-', '') }}/{{ params['placekeys_table'].replace('_','-') }}/",
        )

        clear_gcs_store_visitors_monthly = GCSDeleteObjectsOperator(
            task_id="clear_gcs_store_visitors_monthly",
            bucket_name="{{ params['feeds_staging_gcs_no_pays_bucket'] }}",
            prefix="broadbay/export_date={{ next_ds.replace('-', '') }}/{{ params['store_visitors_table'].replace('_','-') }}/",
        )

        clear_gcs_store_visits_trend_monthly = GCSDeleteObjectsOperator(
            task_id="clear_gcs_store_visits_trend_monthly",
            bucket_name="{{ params['feeds_staging_gcs_no_pays_bucket'] }}",
            prefix="broadbay/export_date={{ next_ds.replace('-', '') }}/{{ params['store_visits_trend_table'].replace('_','-') }}/",
        )

        export_placekeys_monthly = OlvinBigQueryOperator(
            task_id="export_placekeys_monthly",
            query="{% include './bigquery/broadbay/placekeys_monthly.sql' %}",
        )

        export_store_visitors_monthly = OlvinBigQueryOperator(
            task_id="export_store_visitors_monthly",
            query="{% include './bigquery/broadbay/store_visitors_monthly.sql' %}",
            billing_tier="high",
        )

        export_store_visits_trend_monthly = OlvinBigQueryOperator(
            task_id="export_store_visits_trend_monthly",
            query="{% include './bigquery/broadbay/store_visits_trend_monthly.sql' %}",
        )

        clear_s3_start = EmptyOperator(task_id="clear_s3_start")

        clear_s3_store_visits_trend = S3DeleteObjectsOperator(
            task_id="clear_s3_store_visits_trend",
            bucket="{{ params['feeds_staging_aws_bucket_requester_pays_off'] }}",
            prefix="broadbay/export_date={{ next_ds.replace('-', '') }}/{{ params['store_visits_trend_table'].replace('_','-') }}/",
            aws_conn_id="s3_conn",
        )

        clear_s3_store_visitors = S3DeleteObjectsOperator(
            task_id="clear_s3_store_visitors",
            bucket="{{ params['feeds_staging_aws_bucket_requester_pays_off'] }}",
            prefix="broadbay/export_date={{ next_ds.replace('-', '') }}/{{ params['store_visitors_table'].replace('_','-') }}/",
            aws_conn_id="s3_conn",
        )

        clear_s3_placekeys = S3DeleteObjectsOperator(
            task_id="clear_s3_placekeys",
            bucket="{{ params['feeds_staging_aws_bucket_requester_pays_off'] }}",
            prefix="broadbay/export_date={{ next_ds.replace('-', '') }}/{{ params['placekeys_table'].replace('_','-') }}/",
            aws_conn_id="s3_conn",
        )

        source_uri = "gs://{{ params['feeds_staging_gcs_no_pays_bucket'] }}/broadbay/export_date={{ next_ds.replace('-', '') }}"
        dest_uri = "s3a://{{ params['feeds_staging_aws_bucket_requester_pays_off'] }}/broadbay/export_date={{ next_ds.replace('-', '') }}"

        create_export_to_s3_cluster = BashOperator(
            task_id="create_export_to_s3_cluster",
            bash_command=f"gcloud dataproc clusters create {dataproc_cluster_name} "
            f"--region {{{{ params['dataproc_region'] }}}} --master-machine-type n2-standard-4 --master-boot-disk-size 500 "
            "--num-workers 4 --worker-machine-type n2-standard-8 --worker-boot-disk-size 500 "
            "--image-version 1.5-debian10 "
            f"--initialization-actions 'gs://{{{{ params['feeds_keys_gcs_bucket']}}}}/pby-aws-send.sh' "
            f"--project {{{{ var.value.env_project }}}} "
            "--max-age 5h --max-idle 15m",
        )

        submit_export_job = BashOperator(
            task_id="submit_export_job",
            bash_command=f"gcloud dataproc jobs submit hadoop  "
            f"--project={{{{ var.value.env_project }}}} "
            f"--region={dag.params['dataproc_region']} --cluster={dataproc_cluster_name} "
            "--jar file:///usr/lib/hadoop-mapreduce/hadoop-distcp.jar "
            f"-- -strategy dynamic -bandwidth 1000 -update {source_uri} {dest_uri}",
        )

        end = EmptyOperator(
            task_id="end", depends_on_past=False, trigger_rule="all_done"
        )

        (
            branch_task_first_monday
            >> clear_gcs_placekeys_monthly
            >> export_placekeys_monthly
            >> [
                clear_gcs_store_visitors_monthly,
                clear_gcs_store_visits_trend_monthly,
            ]
        )
        clear_gcs_store_visitors_monthly >> export_store_visitors_monthly
        clear_gcs_store_visits_trend_monthly >> export_store_visits_trend_monthly

        (
            [
                export_store_visitors_monthly,
                export_store_visits_trend_monthly,
            ] >> clear_s3_start >> [
                clear_s3_store_visitors,
                clear_s3_store_visits_trend,
                clear_s3_placekeys
            ]
            >> create_export_to_s3_cluster
            >> submit_export_job
            >> end
        )
        (
            branch_task_first_monday
            >> end
        )

    return group
