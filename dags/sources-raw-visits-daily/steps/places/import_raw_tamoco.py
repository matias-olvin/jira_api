"""
DAG ID: visits_pipeline
"""
import datetime

from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyTableOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.sensors.external_task import ExternalTaskSensor


def register(dag, start):
    """Register tasks on the dag.

    Args:
        dag (airflow.models.DAG): DAG instance to register tasks on.
        start (airflow.models.TaskInstance): Task instance all tasks will
            registered downstream from.

    Returns:
        airflow.models.TaskInstance: The last task node in this section.
    """
    # create_empty_table = BigQueryCreateEmptyTableOperator(
    #     task_id="create_empty_table",
    #     project_id="{{ params['project'] }}",  # dag.params['project'],
    #     dataset_id="{{ params['raw_data_dataset'] }}",
    #     table_id="{{ execution_date.strftime('%Y') }}",
    #     schema_fields=[{"name": "event_id", "type": "STRING"},
    #                    {"name": "event_ts", "type": "TIMESTAMP"},
    #                    {"name": "sdk_ts", "type": "TIMESTAMP"},
    #                    {"name": "device_id", "type": "STRING"},
    #                    {"name": "device_os", "type": "STRING"},
    #                    {"name": "device_os_version", "type": "STRING"},
    #                    {"name": "country", "type": "STRING"},
    #                    {"name": "region", "type": "STRING"},
    #                    {"name": "latitude", "type": "FLOAT"},
    #                    {"name": "longitude", "type": "FLOAT"},
    #                    {"name": "accuracy", "type": "FLOAT"},
    #                    {"name": "publisher_id", "type": "INTEGER"}
    #                    ],
    #     time_partitioning={"type": "DAY",
    #                        "field": "sdk_ts"},
    #     labels={
    #         "pipeline": "{{ dag.dag_id }}",
    #         "task_id": "{{ task.task_id.lower()[:63] }}"
    #     },
    #     dag=dag,
    #     location="EU",
    # )

    wait_for_previous_day_data_to_be_deleted = ExternalTaskSensor(
        task_id="wait_for_previous_day_data_to_be_deleted",
        external_dag_id="sources-raw-visits-daily",
        external_task_id="tests_end",
        dag=dag,
        execution_delta=datetime.timedelta(days=1),
        mode="reschedule",
        poke_interval=60 * 10,
        check_existence=True,
    )

    # wait_for_mobilewalla_transfer = GCSObjectExistenceSensor(
    #     task_id='wait_for_mobilewalla_transfer',
    #     bucket=f"{ dag.params['raw_data_bucket'] }",
    #     object=f"{{{{ execution_date.strftime('%Y%m%d') }}}}/_SUCCESS",
    #     dag=dag,
    #     mode="reschedule",
    #     poke_interval=60 * 5
    # )

    wait_for_transfer = GCSObjectExistenceSensor(
        task_id="wait_for_transfer",
        bucket=f"{ dag.params['raw_data_bucket'] }",
        object=f"{{{{ execution_date.strftime('%Y') }}}}/{{{{ execution_date.strftime('%m') }}}}/{{{{ execution_date.strftime('%d') }}}}/_SUCCESS",
        dag=dag,
        mode="reschedule",
        poke_interval=60 * 5,
        timeout=6*60*60,
    )

    load_raw_data = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="load_raw_data",
        project_id=dag.params["project"],
        configuration={
            "load": {
                "sourceUris": [
                    (
                        "gs://{{ params['raw_data_bucket'] }}/"
                        "{{ execution_date.strftime('%Y') }}/"
                        "{{ execution_date.strftime('%m') }}/"
                        "{{ execution_date.strftime('%d') }}/*.parquet"
                    )
                ],
                "destinationTable": {
                    "projectId": "{{ params['project'] }}",
                    "datasetId": "{{ params['raw_data_dataset'] }}",
                    "tableId": "{{ execution_date.strftime('%Y') }}",
                },
                "schema": {
                    "fields": [
                        {
                            "name": "event_id",
                            "mode": "NULLABLE",
                            "type": "STRING",
                            "fields": [],
                        },
                        {
                            "name": "event_ts",
                            "mode": "NULLABLE",
                            "type": "TIMESTAMP",
                            "fields": [],
                        },
                        {
                            "name": "sdk_ts",
                            "mode": "NULLABLE",
                            "type": "TIMESTAMP",
                            "fields": [],
                        },
                        {
                            "name": "device_id",
                            "mode": "NULLABLE",
                            "type": "STRING",
                            "fields": [],
                        },
                        {
                            "name": "device_os",
                            "mode": "NULLABLE",
                            "type": "STRING",
                            "fields": [],
                        },
                        {
                            "name": "device_os_version",
                            "mode": "NULLABLE",
                            "type": "STRING",
                            "fields": [],
                        },
                        {
                            "name": "country",
                            "mode": "NULLABLE",
                            "type": "STRING",
                            "fields": [],
                        },
                        {
                            "name": "region",
                            "mode": "NULLABLE",
                            "type": "STRING",
                            "fields": [],
                        },
                        {
                            "name": "latitude",
                            "mode": "NULLABLE",
                            "type": "FLOAT64",
                            "fields": [],
                        },
                        {
                            "name": "longitude",
                            "mode": "NULLABLE",
                            "type": "FLOAT64",
                            "fields": [],
                        },
                        {
                            "name": "accuracy",
                            "mode": "NULLABLE",
                            "type": "FLOAT64",
                            "fields": [],
                        },
                        {
                            "name": "publisher_id",
                            "mode": "NULLABLE",
                            "type": "INTEGER",
                            "fields": [],
                        },
                    ]
                },
                "sourceFormat": "PARQUET",
                "timePartitioning": {"type": "DAY", "field": "sdk_ts"},
                # "sourceFormat": "CSV",
                # "autodetect": "TRUE",
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_IF_NEEDED",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )

    # merge_raw_tamoco = GCSToGCSOperator(
    #     task_id="merge_raw_tamoco",
    #     source_bucket="{{ params['raw_data_bucket'] }}",
    #     source_object="{{ execution_date.strftime('%Y%m%d') }}/*.parquet",
    #     destination_bucket="{{ params['raw_data_bucket'] }}",
    #     destination_object="{{ execution_date.strftime('%Y/%m/%d') }}/",
    #     move_object=True,
    #     replace=False,
    #     dag=dag,
    # )

    # load_raw_data_mobilewalla = BigQueryInsertJobOperator(
    #     task_id="load_raw_data_mobilewalla",
    #     project_id=dag.params['project'],
    #     configuration={
    #         "load": {
    #             "sourceUris": [
    #                 (
    #                     "gs://{{ params['raw_data_bucket'] }}/"
    #                     "{{ execution_date.strftime('%Y%m%d') }}/"
    #                     "*.parquet"
    #                 )
    #             ],
    #             "destinationTable": {
    #                 "projectId": "{{ params['project'] }}",
    #                 "datasetId": "{{ params['raw_data_dataset'] }}",
    #                 "tableId": "{{ execution_date.strftime('%Y') }}_mobilewalla",
    #             },
    #             "schema": {"fields":
    #                                         [
    #                     {
    #                         "name": "event_id",
    #                         "mode": "NULLABLE",
    #                         "type": "STRING",
    #                         "fields": []
    #                     },
    #                     {
    #                         "name": "event_ts",
    #                         "mode": "NULLABLE",
    #                         "type": "TIMESTAMP",
    #                         "fields": []
    #                     },
    #                     {
    #                         "name": "sdk_ts",
    #                         "mode": "NULLABLE",
    #                         "type": "TIMESTAMP",
    #                         "fields": []
    #                     },
    #                     {
    #                         "name": "device_id",
    #                         "mode": "NULLABLE",
    #                         "type": "STRING",
    #                         "fields": []
    #                     },
    #                     {
    #                         "name": "device_os",
    #                         "mode": "NULLABLE",
    #                         "type": "STRING",
    #                         "fields": []
    #                     },
    #                     {
    #                         "name": "device_os_version",
    #                         "mode": "NULLABLE",
    #                         "type": "STRING",
    #                         "fields": []
    #                     },
    #                     {
    #                         "name": "country",
    #                         "mode": "NULLABLE",
    #                         "type": "STRING",
    #                         "fields": []
    #                     },
    #                     {
    #                         "name": "region",
    #                         "mode": "NULLABLE",
    #                         "type": "STRING",
    #                         "fields": []
    #                     },
    #                     {
    #                         "name": "latitude",
    #                         "mode": "NULLABLE",
    #                         "type": "FLOAT64",
    #                         "fields": []
    #                     },
    #                     {
    #                         "name": "longitude",
    #                         "mode": "NULLABLE",
    #                         "type": "FLOAT64",
    #                         "fields": []
    #                     },
    #                     {
    #                         "name": "accuracy",
    #                         "mode": "NULLABLE",
    #                         "type": "FLOAT64",
    #                         "fields": []
    #                     },
    #                     {
    #                         "name": "publisher_id",
    #                         "mode": "NULLABLE",
    #                         "type": "INTEGER",
    #                         "fields": []
    #                     }
    #                     ]},
    #             "sourceFormat": "PARQUET",
    #             "timePartitioning":{
    #             "type": "DAY",
    #             "field": "sdk_ts"
    #             },
    #             # "sourceFormat": "CSV",
    #             # "autodetect": "TRUE",
    #             "writeDisposition": "WRITE_TRUNCATE",
    #             "createDisposition": "CREATE_IF_NEEDED",
    #         },
    #         "labels": {
    #             "pipeline": "{{ dag.dag_id }}",
    #             "task_id": "{{ task.task_id.lower()[:63] }}"
    #         }
    #     },
    #     dag=dag,
    #     location="EU",
    # )

    # merge_raw_data = BigQueryInsertJobOperator(
    #     task_id="merge_raw_data",
    #     project_id=dag.params['project'],
    #     configuration={
    #         "query": {
    #             "query": "{% include './bigquery/places/merge_raw_data.sql' %}",
    #             "useLegacySql": "False",
    #         },
    #         "labels": {
    #             "pipeline": "{{ dag.dag_id }}",
    #             "task_id": "{{ task.task_id.lower()[:63] }}"
    #         }
    #     },
    #     dag=dag,
    #     location="EU",
    # )

    # load_raw_data = BigQueryInsertJobOperator(
    #     task_id="load_raw_data",
    #     project_id=dag.params['project'],
    #     configuration={
    #         "load": {
    #             "sourceUris": [
    #                 (
    #                     "gs://{{ params['raw_data_bucket'] }}/"
    #                     "{{ execution_date.strftime('%Y') }}/"
    #                     "{{ execution_date.strftime('%m') }}/"
    #                     "{{ execution_date.strftime('%d') }}/*.snappy.parquet"
    #                 )
    #             ],
    #             "destinationTable": {
    #                 "projectId": "{{ params['project'] }}",
    #                 "datasetId": "{{ params['raw_data_dataset'] }}",
    #                 "tableId": "{{ execution_date.strftime('%Y') }}",
    #             },
    #             "sourceFormat": "PARQUET",
    #             "timePartitioning":{
    #             "type": "DAY",
    #             "field": "sdk_ts"
    #             },
    #             "writeDisposition": "WRITE_APPEND",
    #             "createDisposition": "CREATE_IF_NEEDED",
    #         },
    #         "labels": {
    #             "pipeline": "{{ dag.dag_id }}",
    #             "task_id": "{{ task.task_id.lower()[:63] }}"
    #         }
    #     },
    #     dag=dag,
    #     location="EU",
    # )

    (
        start
        >> wait_for_previous_day_data_to_be_deleted
        >> wait_for_transfer
        >> load_raw_data
    )
    # create_empty_table >> load_raw_data

    return load_raw_data
