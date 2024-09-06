from __future__ import annotations

from typing import Union

from airflow.models import DAG, TaskInstance
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup


def register(
    start: Union[TaskGroup, TaskInstance], dag: DAG, sample_bucket_name: str
) -> TaskInstance:
    """
    Register tasks on the dag

    Args:
        start (airflow.models.TaskInstance): Task instance all tasks will
        be registered downstream from.
        dag (airflow.models.DAG): The dag to register the tasks on.

    Returns:
        airflow.models.TaskInstance: The task instance that the next task
        will be registered downstream from.
    """

    DATAPROC_CLUSTER_NAME = f"pby-product-p-dpc-products-feeds-buckets-sample-aws"

    clear_s3_sample_bucket = BashOperator(
        task_id="clear_s3_sample_bucket",
        bash_command="{% include './include/bash/empty_entire_s3_bucket.sh' %}",
        env={
            "S3_DESTINATION_BUCKET": sample_bucket_name,
            "GCS_KEYS_BUCKET": "{{ params['feeds_keys_gcs_bucket'] }}",
            "CLIENT_KEYFILE": "pby-aws-send",
        }
    )

    create_export_to_s3_cluster = BashOperator(
        task_id="create_export_to_s3_cluster",
        bash_command=f"gcloud dataproc clusters create {DATAPROC_CLUSTER_NAME} "
        f"--region europe-west1 --master-machine-type n2-standard-4 --master-boot-disk-size 500 "
        "--num-workers 4 --worker-machine-type n2-standard-8 --worker-boot-disk-size 500 "
        "--image-version 1.5-debian10 "
        f"--initialization-actions 'gs://pby-product-p-gcs-euwe1-datafeed-keys/pby-aws-send.sh' "
        "--project {{ var.value.env_project }} "
        "--max-age 5h --max-idle 15m",
    )

    source_uri = "s3a://{{ params['data_feeds_s3_bucket'] }}/general/type-1"
    dest_uri = f"s3a://{sample_bucket_name}/export_date={{{{ dag_run.conf['export_date_string_no_dash'] }}}}/general"

    copy_s3_to_s3_general_sample = BashOperator(
        task_id="copy_s3_to_s3_general_sample",
        bash_command=f"gcloud dataproc jobs submit hadoop  "
        "--project={{ var.value.env_project }} "
        f"--region=europe-west1 --cluster={DATAPROC_CLUSTER_NAME} "
        "--jar file:///usr/lib/hadoop-mapreduce/hadoop-distcp.jar "
        f"-- -strategy dynamic -bandwidth 1000 -update {source_uri} {dest_uri}",
    )

    source_uri = "s3a://{{ params['data_feeds_s3_bucket'] }}/finance/type-1"
    dest_uri = f"s3a://{sample_bucket_name}/export_date={{{{ dag_run.conf['export_date_string_no_dash'] }}}}/finance"

    copy_s3_to_s3_finance_sample = BashOperator(
        task_id="copy_s3_to_s3_finance_sample",
        bash_command=f"gcloud dataproc jobs submit hadoop  "
        "--project={{ var.value.env_project }} "
        f"--region=europe-west1 --cluster={DATAPROC_CLUSTER_NAME} "
        "--jar file:///usr/lib/hadoop-mapreduce/hadoop-distcp.jar "
        f"-- -strategy dynamic -bandwidth 1000 -update {source_uri} {dest_uri}",
    )

    (
        start
        >> clear_s3_sample_bucket
        >> create_export_to_s3_cluster
        >> copy_s3_to_s3_general_sample
        >> copy_s3_to_s3_finance_sample
    )

    return copy_s3_to_s3_finance_sample
