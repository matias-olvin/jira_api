from airflow.models import DAG, TaskInstance, Variable
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.utils.task_group import TaskGroup
from common.operators.bigquery import OlvinBigQueryOperator


def register(
    start: TaskInstance, dag: DAG, dag_id: str, dataproc_name: str
) -> TaskInstance:

    # Exporting the data to GCS.
    PARTITION_COLUMN = "local_date"
    BACKFILL_START_DATE = f"{dag.params['backfill_start_date']}"

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

    with TaskGroup(group_id="data-feed-export-daily-create-tables") as create_tables:

        data_feed_export_daily_backfill_create_table = OlvinBigQueryOperator(
            task_id="data-feed-export-daily-backfill-create-table",
            query="{% include './bigquery/daily_tables/create_store_visits_daily.sql' %}",
            billing_tier="high",
        )

        data_feed_export_daily_backfill_finance_create_table = OlvinBigQueryOperator(
            task_id="data-feed-export-daily-backfill-finance-create-table",
            query="{% include './bigquery/daily_tables/create_store_visits_daily_finance.sql' %}",
            billing_tier="high",
        )

        (
            data_feed_export_daily_backfill_create_table
            >> data_feed_export_daily_backfill_finance_create_table
        )

    create_export_to_gcs_cluster = DataprocCreateClusterOperator(
        task_id="create_export_to_gcs_cluster_daily",
        project_id=Variable.get("env_project"),
        cluster_config=cluster_config,
        region=dag.params["dataproc_region"],
        cluster_name=dataproc_name,
        dag=dag,
    )

    with TaskGroup(group_id="data-feed-export-daily-backfill") as feed_export_daily:

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
                        f"--date_end={{{{ execution_date.add(months=12).strftime('%Y-%m-01') }}}}",
                    ],
                },
            },
            region=dag.params["dataproc_region"],
            project_id=Variable.get("env_project"),
            dag=dag,
        )

        clear_gcs >> submit_export_to_gcs_job

    with TaskGroup(
        group_id="data-feed-export-daily-backfill-finance"
    ) as feed_export_daily_finance:

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
                        f"--date_end={{{{ execution_date.add(months=12).strftime('%Y-%m-01') }}}}",
                    ],
                },
            },
            region=dag.params["dataproc_region"],
            project_id=Variable.get("env_project"),
            dag=dag,
        )

        clear_gcs >> submit_export_to_gcs_job

    # Exporting the data to GCS for versioning
    clear_store_visits_daily_for_versioning = GCSDeleteObjectsOperator(
            task_id="clear_store_visits_daily_for_versioning",
            bucket_name="{{ params['versioning_bucket'] }}",
            prefix="type-1/{{ params['store_visits_daily_table'] }}/{{ next_ds.replace('-', '/') }}/",
    )

    export_store_visits_daily_for_versioning = OlvinBigQueryOperator(
        task_id="export_store_visits_daily_for_versioning",
        query="{% include './bigquery/export_store_visits_daily_for_versioning.sql' %}",
        billing_tier="high",
    )

    delete_cluster_daily = DataprocDeleteClusterOperator(
        task_id="delete_cluster_daily",
        project_id=Variable.get("env_project"),
        cluster_name=dataproc_name,
        region=dag.params["dataproc_region"],
        dag=dag,
    )

    (
        start
        >> create_tables
        >> create_export_to_gcs_cluster
        >> feed_export_daily
        >> feed_export_daily_finance
        >> delete_cluster_daily
    )

    create_tables >> clear_store_visits_daily_for_versioning >> export_store_visits_daily_for_versioning

    return delete_cluster_daily
