from typing import Optional

from airflow.models import TaskInstance, DAG
from airflow.providers.google.cloud.transfers.gcs_to_sftp import GCSToSFTPOperator

from common.operators.bigquery import OlvinBigQueryOperator


def register(start: TaskInstance, dag: Optional[DAG]) -> TaskInstance:
    export_job = OlvinBigQueryOperator(
        task_id="export_to_GCS_sns_",
        query="{% include './include/bigquery/export_visits.sql' %}"
    )
    move_file_from_gcs_to_sftp = GCSToSFTPOperator(
        task_id="file-move-gsc-to-sftp",
        sftp_conn_id="sftp_default",
        source_bucket="{{ params['sns_transfer_bucket'] }}",
        source_object="export/*.csv",
        destination_path="{{ ds_nodash }}",
        move_object=False,
    )

    start >> export_job
    export_job >> move_file_from_gcs_to_sftp

    return move_file_from_gcs_to_sftp
