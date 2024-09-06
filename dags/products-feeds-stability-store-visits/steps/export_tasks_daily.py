from datetime import datetime
from typing import Union

from airflow.models import DAG, TaskInstance, Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.utils.task_group import TaskGroup
from common.operators.bigquery import OlvinBigQueryOperator


def register(
    start: Union[TaskGroup, TaskInstance],
    dag: DAG,
    dataproc_name: str,
    dag_id: str,
) -> TaskInstance:

    BACKFILL_START_DATE = f"{dag.params['backfill_start_date']}"
    PARTITION_COLUMN = "local_date"

    def branch_task_insert_fn(**kwargs):

        execution_date = kwargs["execution_date"]
        if not isinstance(execution_date, datetime):
            execution_date = datetime.strptime(execution_date, "%Y-%m-%d")

        smc_end_date_prev = Variable.get("smc_end_date_prev")
        if not isinstance(smc_end_date_prev, datetime):
            smc_end_date_prev = datetime.strptime(smc_end_date_prev, "%Y-%m-%d")

        if execution_date.date() >= smc_end_date_prev.date():
            return "start_increment_daily"
        else:
            interval = (smc_end_date_prev.date() - execution_date.date()).days
            if interval > 24:
                return "start_increment_daily"
            else:
                return "start_backfill_daily"

    cluster_config = {
        "worker_config": {
            "num_instances": 8,
            "machine_type_uri": "n1-highmem-16",
            "disk_config": {
                "boot_disk_type": "pd-ssd",
                "boot_disk_size_gb": 1024,
                "local_ssd_interface": "nvme",
                "num_local_ssds": 1,
            },
        },
        "initialization_actions": [
            {
                "executable_file": (
                    "gs://goog-dataproc-initialization-actions-"
                    "{{ params['dataproc_region'] }}/connectors/connectors.sh"
                )
            }
        ],
        "gce_cluster_config": {
            "metadata": {
                "bigquery-connector-version": "1.2.0",
                "spark-bigquery-connector-version": "0.35.0",
            },
        },
        "lifecycle_config": {
            "auto_delete_ttl": {"seconds": 7200},
            "idle_delete_ttl": {"seconds": 3600},
        },
        "software_config": {
            "image_version": "2.0-ubuntu18",
            "properties": {
                "spark:spark.sql.shuffle.partitions": "384",  # num vCPUS available * 3 is recommended
                "spark:spark.sql.adaptive.enabled": "true",
                "spark:spark.dynamicAllocation.enabled": "true",
                "dataproc:efm.spark.shuffle": "primary-worker",
                "spark:spark.dataproc.enhanced.optimizer.enabled": "true",
                "spark:spark.dataproc.enhanced.execution.enabled": "true",
                "hdfs:dfs.replication": "1",
            },
        },
        "endpoint_config": {
            "enable_http_port_access": True,
        },
    }

    branch_task_insert = BranchPythonOperator(
        task_id="branch_task_insert_daily",
        provide_context=True,
        python_callable=branch_task_insert_fn,
        dag=dag,
    )

    start_increment_daily = EmptyOperator(task_id="start_increment_daily")
    start_backfill_daily = EmptyOperator(task_id="start_backfill_daily")

    with TaskGroup(
        group_id="data-feed-export-backfill-create-daily-tables"
    ) as feed_export_backfill_create_daily_tables:

        create_backfill_tables = EmptyOperator(task_id="create_backfill_tables")

        data_feed_export_daily_backfill_create_table = OlvinBigQueryOperator(
            task_id="data-feed-export-daily-backfill-create-table",
            query="{% include './bigquery/store_visits_daily/create_store_visits_daily_backfill.sql' %}",
            billing_tier="high",
        )

        data_feed_export_daily_finance_backfill_create_table = OlvinBigQueryOperator(
            task_id="data-feed-export-daily-finance-backfill-create-table",
            query="{% include './bigquery/store_visits_daily/create_store_visits_daily_finance.sql' %}",
            billing_tier="high",
        )

        (
            create_backfill_tables
            >> data_feed_export_daily_backfill_create_table
            >> data_feed_export_daily_finance_backfill_create_table
        )

    with TaskGroup(
        group_id="data-feed-export-increment-create-daily-tables"
    ) as feed_export_increment_create_daily_tables:

        create_increment_tables = EmptyOperator(task_id="create_increment_tables")

        data_feed_export_daily_increment_create_table = OlvinBigQueryOperator(
            task_id="data-feed-export-daily-increment-create-table",
            query="{% include './bigquery/store_visits_daily/create_store_visits_daily_increment.sql' %}",
            billing_tier="high",
        )

        data_feed_export_daily_finance_increment_create_table = OlvinBigQueryOperator(
            task_id="data-feed-export-daily-finance-increment-create-table",
            query="{% include './bigquery/store_visits_daily/create_store_visits_daily_finance.sql' %}",
            billing_tier="high",
        )

        (
            create_increment_tables
            >> data_feed_export_daily_increment_create_table
            >> data_feed_export_daily_finance_increment_create_table
        )

    create_export_to_gcs_cluster_daily_increment = DataprocCreateClusterOperator(
        task_id="create_export_to_gcs_cluster_daily_increment",
        project_id=Variable.get("env_project"),
        cluster_config=cluster_config,
        region=dag.params["dataproc_region"],
        cluster_name=dataproc_name,
        dag=dag,
    )

    create_export_to_gcs_cluster_daily_backfill = DataprocCreateClusterOperator(
        task_id="create_export_to_gcs_cluster_daily_backfill",
        project_id=Variable.get("env_project"),
        cluster_config=cluster_config,
        region=dag.params["dataproc_region"],
        cluster_name=dataproc_name,
        dag=dag,
    )

    with TaskGroup(
        group_id="data-feed-export-daily-backfill"
    ) as feed_export_daily_backfill:

        clear_gcs = GCSDeleteObjectsOperator(
            task_id="clear_gcs",
            depends_on_past=False,
            bucket_name="{{ params['raw_feed_bucket'] }}",
            prefix="{{ params['store_visits_daily_table'].replace('_','-') }}/",
        )

        submit_export_to_gcs_job = DataprocSubmitJobOperator(
            task_id="submit_export_to_gcs_job",
            depends_on_past=False,
            job={
                "reference": {"project_id": "{{ var.value.env_project }}"},
                "placement": {"cluster_name": dataproc_name},
                "pyspark_job": {
                    "main_python_file_uri": f"{{{{ var.value.gcs_dags_folder }}}}/{dag_id}/pyspark/runner.py",
                    "python_file_uris": [
                        f"{{{{ var.value.gcs_dags_folder }}}}/{dag_id}/pyspark/args.py",
                    ],
                    "args": [
                        f"--project={{{{ var.value.env_project }}}}",
                        f"--input_table={{{{ params['public_feeds_dataset'] }}}}.{{{{ params['store_visits_daily_table'] }}}}",
                        f"--output_folder=gs://{{{{ params['raw_feed_bucket'] }}}}/{{{{ params['store_visits_daily_table'].replace('_','-') }}}}",
                        f"--partition_column={PARTITION_COLUMN}",
                        f"--num_files_per_partition=7",
                        f"--append_mode=overwrite",
                        f"--date_start={BACKFILL_START_DATE}",
                        f"--date_end={{{{ execution_date.add(months=1).strftime('%Y-%m-01') }}}}",
                    ],
                },
            },
            region=dag.params["dataproc_region"],
            project_id=Variable.get("env_project"),
            dag=dag,
        )

        clear_gcs >> submit_export_to_gcs_job

    with TaskGroup(
        group_id="data-feed-export-daily-increment"
    ) as feed_export_daily_increment:

        clear_gcs = GCSDeleteObjectsOperator(
            task_id="clear_gcs",
            depends_on_past=False,
            bucket_name="{{ params['raw_feed_bucket'] }}",
            prefix=f"{{{{ params['store_visits_daily_table'].replace('_','-') }}}}/{PARTITION_COLUMN}={{{{ execution_date.strftime('%Y-%m') }}}}",
        )

        submit_export_to_gcs_job = DataprocSubmitJobOperator(
            task_id="submit_export_to_gcs_job",
            depends_on_past=False,
            job={
                "reference": {"project_id": "{{ var.value.env_project }}"},
                "placement": {"cluster_name": dataproc_name},
                "pyspark_job": {
                    "main_python_file_uri": f"{{{{ var.value.gcs_dags_folder }}}}/{dag_id}/pyspark/runner.py",
                    "python_file_uris": [
                        f"{{{{ var.value.gcs_dags_folder }}}}/{dag_id}/pyspark/args.py",
                    ],
                    "args": [
                        f"--project={{{{ var.value.env_project }}}}",
                        f"--input_table={{{{ params['public_feeds_dataset'] }}}}.{{{{ params['store_visits_daily_table'] }}}}",
                        f"--output_folder=gs://{{{{ params['raw_feed_bucket'] }}}}/{{{{ params['store_visits_daily_table'].replace('_','-') }}}}",
                        f"--partition_column={PARTITION_COLUMN}",
                        f"--num_files_per_partition=7",
                        f"--append_mode=append",
                        f"--date_start={{{{ execution_date.strftime('%Y-%m-01') }}}}",
                        f"--date_end={{{{ execution_date.add(months=1).strftime('%Y-%m-01') }}}}",
                    ],
                },
            },
            region=dag.params["dataproc_region"],
            project_id=Variable.get("env_project"),
            dag=dag,
        )

        clear_gcs >> submit_export_to_gcs_job

    with TaskGroup(
        group_id="data-feed-export-daily-finance-backfill"
    ) as feed_export_daily_finance_backfill:

        clear_gcs = GCSDeleteObjectsOperator(
            task_id="clear_gcs",
            depends_on_past=False,
            bucket_name="{{ params['raw_feed_finance_bucket'] }}",
            prefix="{{ params['store_visits_daily_table'].replace('_','-') }}/",
        )

        submit_export_to_gcs_job = DataprocSubmitJobOperator(
            task_id="submit_export_to_gcs_job",
            depends_on_past=False,
            job={
                "reference": {"project_id": "{{ var.value.env_project }}"},
                "placement": {"cluster_name": dataproc_name},
                "pyspark_job": {
                    "main_python_file_uri": f"{{{{ var.value.gcs_dags_folder }}}}/{dag_id}/pyspark/runner.py",
                    "python_file_uris": [
                        f"{{{{ var.value.gcs_dags_folder }}}}/{dag_id}/pyspark/args.py",
                    ],
                    "args": [
                        f"--project={{{{ var.value.env_project }}}}",
                        f"--input_table={{{{ params['public_feeds_finance_dataset'] }}}}.{{{{ params['store_visits_daily_table'] }}}}",
                        f"--output_folder=gs://{{{{ params['raw_feed_finance_bucket'] }}}}/{{{{ params['store_visits_daily_table'].replace('_','-') }}}}",
                        f"--partition_column={PARTITION_COLUMN}",
                        f"--num_files_per_partition=7",
                        f"--append_mode=overwrite",
                        f"--date_start={BACKFILL_START_DATE}",
                        f"--date_end={{{{ execution_date.add(months=1).strftime('%Y-%m-01') }}}}",
                    ],
                },
            },
            region=dag.params["dataproc_region"],
            project_id=Variable.get("env_project"),
            dag=dag,
        )

        clear_gcs >> submit_export_to_gcs_job

    with TaskGroup(
        group_id="data-feed-export-daily-finance-increment"
    ) as feed_export_daily_finance_increment:

        clear_gcs = GCSDeleteObjectsOperator(
            task_id="clear_gcs",
            depends_on_past=False,
            bucket_name="{{ params['raw_feed_finance_bucket'] }}",
            prefix=f"{{{{ params['store_visits_daily_table'].replace('_','-') }}}}/{PARTITION_COLUMN}={{{{ execution_date.strftime('%Y-%m') }}}}",
        )

        submit_export_to_gcs_job = DataprocSubmitJobOperator(
            task_id="submit_export_to_gcs_job",
            depends_on_past=False,
            job={
                "reference": {"project_id": "{{ var.value.env_project }}"},
                "placement": {"cluster_name": dataproc_name},
                "pyspark_job": {
                    "main_python_file_uri": f"{{{{ var.value.gcs_dags_folder }}}}/{dag_id}/pyspark/runner.py",
                    "python_file_uris": [
                        f"{{{{ var.value.gcs_dags_folder }}}}/{dag_id}/pyspark/args.py",
                    ],
                    "args": [
                        f"--project={{{{ var.value.env_project }}}}",
                        f"--input_table={{{{ params['public_feeds_finance_dataset'] }}}}.{{{{ params['store_visits_daily_table'] }}}}",
                        f"--output_folder=gs://{{{{ params['raw_feed_finance_bucket'] }}}}/{{{{ params['store_visits_daily_table'].replace('_','-') }}}}",
                        f"--partition_column={PARTITION_COLUMN}",
                        f"--num_files_per_partition=7",
                        f"--append_mode=append",
                        f"--date_start={{{{ execution_date.strftime('%Y-%m-01') }}}}",
                        f"--date_end={{{{ execution_date.add(months=1).strftime('%Y-%m-01') }}}}",
                    ],
                },
            },
            region=dag.params["dataproc_region"],
            project_id=Variable.get("env_project"),
            dag=dag,
        )

        clear_gcs >> submit_export_to_gcs_job

    # Exporting the data to GCS for versioning
    clear_store_visits_daily_for_versioning_backfill = GCSDeleteObjectsOperator(
        task_id="clear_store_visits_daily_for_versioning_backfill",
        bucket_name="{{ params['versioning_bucket'] }}",
        prefix="type-2/{{ params['store_visits_daily_table'] }}/{{ next_ds.replace('-', '/') }}/",
    )

    export_store_visits_daily_for_versioning_backfill = OlvinBigQueryOperator(
        task_id="export_store_visits_daily_for_versioning_backfill",
        query="{% include './bigquery/export_store_visits_daily_for_versioning_backfill.sql' %}",
        billing_tier="high",
    )

    clear_store_visits_daily_for_versioning_increment = GCSDeleteObjectsOperator(
        task_id="clear_store_visits_daily_for_versioning_increment",
        bucket_name="{{ params['versioning_bucket'] }}",
        prefix="type-2/{{ params['store_visits_daily_table'] }}/{{ next_ds.replace('-', '/') }}/",
    )

    export_store_visits_daily_for_versioning_increment = OlvinBigQueryOperator(
        task_id="export_store_visits_daily_for_versioning_increment",
        query="{% include './bigquery/export_store_visits_daily_for_versioning_increment.sql' %}",
        billing_tier="high",
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster_daily",
        project_id=Variable.get("env_project"),
        cluster_name=dataproc_name,
        region=dag.params["dataproc_region"],
        dag=dag,
        trigger_rule="none_failed_min_one_success",
    )

    (
        start
        >> branch_task_insert
        >> start_increment_daily
        >> feed_export_increment_create_daily_tables
        >> create_export_to_gcs_cluster_daily_increment
        >> feed_export_daily_increment
        >> feed_export_daily_finance_increment
        >> delete_cluster
    )

    (
        feed_export_increment_create_daily_tables
        >> clear_store_visits_daily_for_versioning_increment
        >> export_store_visits_daily_for_versioning_increment
    )

    (
        start
        >> branch_task_insert
        >> start_backfill_daily
        >> feed_export_backfill_create_daily_tables
        >> create_export_to_gcs_cluster_daily_backfill
        >> feed_export_daily_backfill
        >> feed_export_daily_finance_backfill
        >> delete_cluster
    )

    (
        feed_export_backfill_create_daily_tables
        >> clear_store_visits_daily_for_versioning_backfill
        >> export_store_visits_daily_for_versioning_backfill
    )

    end = EmptyOperator(task_id="end_daily_exports")

    delete_cluster >> end

    return end
