import os, glob
from datetime import datetime


from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryValueCheckOperator,
)
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.utils.task_group import TaskGroup
from common.operators.bigquery import OlvinBigQueryOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.sensors.external_task import ExternalTaskSensor

from common.utils import dag_args, callbacks, slack_users

folder_path = f"{os.path.dirname(os.path.realpath(__file__))}"
DAG_ID = folder_path.split("/")[-1]


env_args = dag_args.make_env_args(
    schedule_interval="0 6 1 * *"
)
default_args=dag_args.make_default_args(
    start_date=datetime(2022, 9, 16),
    depends_on_past=True,
    wait_for_downstream=True,
    retries=3,
    on_failure_callback=callbacks.task_fail_slack_alert(
        slack_users.MATIAS, channel=env_args["env"]
    ),
)

with DAG(
    DAG_ID,
    default_args=default_args,
    schedule_interval=env_args["schedule_interval"],
    params=dag_args.load_config(__file__),
    doc_md=dag_args.load_docs(__file__),
) as dag:
    start = DummyOperator(task_id="start")
    insert_placekeys_into_temp = BigQueryInsertJobOperator(
        task_id="insert_placekeys_into_temp",
        configuration={
            "query": {
                "query": "{% include './bigquery/placekeys_temp.sql' %}",
                "useLegacySql": "false",
            }
        },
        params=dag.params,
    )
    
    create_snapshot_places = BigQueryInsertJobOperator(
        task_id="create_snapshot_places",
        configuration={
            "query": {
                "query": "{% include './bigquery/create_snapshot_places.sql' %}",
                "useLegacySql": "false",
            }
        },
        params=dag.params,
    )

    remove_placekeys_temp = BigQueryInsertJobOperator(
        task_id="remove_placekeys_temp",
        gcp_conn_id="google_cloud_olvin_default",
        configuration={
            "query": {
                "query": "{% include './bigquery/remove_placekeys_temp.sql' %}",
                "useLegacySql": "false",
            }
        },
    )


    for feed in {"store_visits_feed", "store_visitors_feed", "store_visits_trend_feed"}:
        sensor = ExternalTaskSensor(
            task_id=f"{feed}_sensor",
            external_dag_id=f"{{{{ params['{feed}'] }}}}",
            external_task_id="end",
            execution_delta=0,
            mode="reschedule",
            poke_interval=60 * 5,
            timeout=60 * 60 * 24,
        )
        start >> sensor >> create_snapshot_places
    create_snapshot_places >> insert_placekeys_into_temp >> remove_placekeys_temp
    # data quality checks and export
    data_quality_start = DummyOperator(task_id="data_quality_start")
    data_quality_end = DummyOperator(task_id="data_quality_end")

    remove_placekeys_temp >> data_quality_start

    # Looping through all the files in the data_quality folder and running the data quality checks.
    for file in glob.glob(
        f"{os.path.dirname(os.path.relpath(__file__))}/bigquery/data_quality/*.sql"
    ):
        # Taking the file name and removing the .sql extension.
        file_name = str(file).split("/")[-1].replace(".sql", "")
        data_quality_task = BigQueryValueCheckOperator(
            task_id=f"{file_name}",
            gcp_conn_id="google_cloud_olvin_default",
            sql=f"{{% include './bigquery/data_quality/{file_name}.sql' %}}",
            use_legacy_sql=False,
            pass_value=0,
            params=dag.params,
        )
        data_quality_start >> data_quality_task >> data_quality_end

    # Exporting the data to GCS.
    with TaskGroup(group_id="data-feed-export-backfill") as feed_export:
        insert_placekeys = BigQueryInsertJobOperator(
        task_id="insert_placekeys",
        depends_on_past=False,
        configuration={
            "query": {
                "query": "{% include './bigquery/placekeys_backfill.sql' %}",
                "useLegacySql": "false",
            }
        },
        params=dag.params,
    )

        clear_gcs = GCSDeleteObjectsOperator(
        task_id="clear_gcs",
        depends_on_past=False,
        gcp_conn_id="google_cloud_olvin_default",
        bucket_name="{{ params['raw_feed_bucket'] }}",
        prefix="{{ params['placekeys_table'] }}/",
    )
        
        export_to_gcs = BigQueryInsertJobOperator(
        task_id="export_to_gcs",
        depends_on_past=False,
        gcp_conn_id="google_cloud_olvin_default",
        configuration={
            "extract": {
                "destinationUri": "gs://{{ params['raw_feed_bucket'] }}/{{ params['placekeys_table']}}/*.zstd.parquet",
                "destinationFormat": "PARQUET",
                "compression": "ZSTD",
                "sourceTable": {
                    "projectId": "{{ var.value.env_project }}",
                    "datasetId": "{{ params['public_feeds_dataset'] }}",
                    "tableId": "{{ params['placekeys_table'] }}",
                },
            }
        },
    )

        insert_placekeys >> clear_gcs >> export_to_gcs

    with TaskGroup(group_id="data-feed-export-backfill-finance") as feed_export_finance:
        insert_placekeys = BigQueryInsertJobOperator(
        task_id="insert_placekeys",
        depends_on_past=False,
        configuration={
            "query": {
                "query": "{% include './bigquery/placekeys_finance_backfill.sql' %}",
                "useLegacySql": "false",
            }
        },
        params=dag.params,
    )

        clear_gcs = GCSDeleteObjectsOperator(
        task_id="clear_gcs",
        depends_on_past=False,
        gcp_conn_id="google_cloud_olvin_default",
        bucket_name="{{ params['raw_feed_finance_bucket'] }}",
        prefix="{{ params['placekeys_table'] }}/",
    )
        
        export_to_gcs = BigQueryInsertJobOperator(
        task_id="export_to_gcs",
        depends_on_past=False,
        gcp_conn_id="google_cloud_olvin_default",
        configuration={
            "extract": {
                "destinationUri": "gs://{{ params['raw_feed_finance_bucket'] }}/{{ params['placekeys_table']}}/*.zstd.parquet",
                "destinationFormat": "PARQUET",
                "compression": "ZSTD",
                "sourceTable": {
                    "projectId": "{{ var.value.env_project }}",
                    "datasetId": "{{ params['public_feeds_finance_dataset'] }}",
                    "tableId": "{{ params['placekeys_table'] }}",
                },
            }
        },
    )

        insert_placekeys >> clear_gcs >> export_to_gcs

    # Exporting the data to GCS for versioning
    clear_placekeys_for_versioning = GCSDeleteObjectsOperator(
            task_id="clear_placekeys_for_versioning",
            bucket_name="{{ params['versioning_bucket'] }}",
            prefix="type-1/{{ params['placekeys_table'] }}/{{ next_ds.replace('-', '/') }}/",
        )

    export_placekeys_for_versioning = OlvinBigQueryOperator(
        task_id="export_placekeys_for_versioning",
        query="{% include './bigquery/export_placekeys_for_versioning.sql' %}",
    )

    end = DummyOperator(task_id="end")

    data_quality_end >> feed_export >> feed_export_finance >> end

    feed_export >> clear_placekeys_for_versioning >> export_placekeys_for_versioning
