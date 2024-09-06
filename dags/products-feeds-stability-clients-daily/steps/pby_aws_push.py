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
    with TaskGroup("pby_aws_push") as pby_aws_push_group:

        client_details = [
            {
                "name": "maiden-century",
                "init_actions_file_name": "pby-aws-send-maiden-century.sh",
                "source_uri": "gs://{{ params['feeds_staging_gcs_daily_bucket'] }}/export_date={{ next_ds.replace('-', '') }}/type_2/{{ params['store_visits_daily_table'].replace('_','-') }}/finance/parquet_format",
                "dest_uri": "s3a://{{ params['maiden_century_bucket'] }}/store_visits/export_date={{ next_ds.replace('-', '') }}",
            },
            {
                "name": "quantcube",
                "init_actions_file_name": "pby-aws-send-quantcube.sh",
                "source_uri": "gs://{{ params['feeds_staging_gcs_daily_bucket'] }}/export_date={{ next_ds.replace('-', '') }}/type_2/{{ params['store_visits_daily_table'].replace('_','-') }}/finance/parquet_format",
                "dest_uri": "s3a://{{ params['quantcube_bucket'] }}/poi_data/passby_live/export_date={{ next_ds.replace('-', '') }}",
            },
        ]

        for details in client_details:

            # fmt: off
            dataproc_cluster_name = f"pby-product-p-dpc-euwe1-s3-stab-push-{details['name']}"
            # fmt: on

            create_export_to_s3_cluster = BashOperator(
                task_id=f"create_export_to_s3_cluster_{details['name']}",
                bash_command=f"gcloud dataproc clusters create {dataproc_cluster_name} "
                f"--region {{{{ params['dataproc_region'] }}}} --master-machine-type n2-standard-4 --master-boot-disk-size 500 "
                "--num-workers 4 --worker-machine-type n2-standard-8 --worker-boot-disk-size 500 "
                "--image-version 1.5-debian10 "
                f"--initialization-actions 'gs://{{{{ params['feeds_keys_gcs_bucket']}}}}/{details['init_actions_file_name']}' "
                f"--project {{{{ var.value.env_project }}}} "
                "--max-age 5h --max-idle 15m",
            )

            submit_export_job = BashOperator(
                task_id=f"submit_export_job_{details['name']}",
                bash_command=f"gcloud dataproc jobs submit hadoop  "
                f"--project={{{{ var.value.env_project }}}} "
                f"--region={dag.params['dataproc_region']} --cluster={dataproc_cluster_name} "
                "--jar file:///usr/lib/hadoop-mapreduce/hadoop-distcp.jar "
                f"-- -strategy dynamic -bandwidth 1000 -update {details['source_uri']} {details['dest_uri']}",
            )

            start >> create_export_to_s3_cluster >> submit_export_job

    return pby_aws_push_group
