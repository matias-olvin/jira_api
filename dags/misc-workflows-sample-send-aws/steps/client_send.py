from __future__ import annotations

from typing import Union

from airflow.models import DAG, TaskInstance
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup


def register(start: Union[TaskGroup, TaskInstance], dag: DAG) -> TaskGroup:
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
    with TaskGroup("pby_aws_push") as pby_aws_pull_group:

        DATAPROC_CLUSTER_NAME = f"pby-product-p-dpc-misc-maiden-century"

        create_export_to_s3_cluster = BashOperator(
            task_id="create_export_to_s3_cluster",
            bash_command=f"gcloud dataproc clusters create {DATAPROC_CLUSTER_NAME} "
            f"--region europe-west1 --master-machine-type n2-standard-4 --master-boot-disk-size 500 "
            "--num-workers 4 --worker-machine-type n2-standard-8 --worker-boot-disk-size 500 "
            "--image-version 1.5-debian10 "
            f"--initialization-actions 'gs://pby-product-p-gcs-euwe1-datafeed-keys/pby-aws-send-misc-maiden-century.sh' "
            f"--project storage-dev-olvin-com "
            "--max-age 5h --max-idle 15m",
        )

        source_uri = "gs://sample-feed/maiden_century/daily_data"
        dest_uri = "s3a://mc-ext-vendor-passby/daily_data_resend"

        submit_export_job = BashOperator(
            task_id="submit_export_job",
            bash_command=f"gcloud dataproc jobs submit hadoop  "
            f"--project=storage-dev-olvin-com "
            f"--region=europe-west1 --cluster={DATAPROC_CLUSTER_NAME} "
            "--jar file:///usr/lib/hadoop-mapreduce/hadoop-distcp.jar "
            f"-- -overwrite {source_uri} {dest_uri}",
        )

        start >> create_export_to_s3_cluster >> submit_export_job

    return pby_aws_pull_group
