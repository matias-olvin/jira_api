from __future__ import annotations

from typing import Union

from airflow.models import DAG, TaskInstance
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator
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
    with TaskGroup("pby_aws_pull") as pby_aws_pull_group:

        client_names_list = [
            {
                "client_name": "broadbay",
                "source_uri": "gs://{{ params['feeds_staging_gcs_daily_bucket'] }}/export_date={{ next_ds.replace('-', '') }}/type_1/{{ params['store_visits_daily_table'].replace('_','-') }}/historical_only/finance/parquet_format",
                "requester_pays_destination_bucket": False,
            },
        ]

        for client_record in client_names_list:

            destination_bucket = "{{ params['feeds_staging_aws_bucket'] }}" if client_record['requester_pays_destination_bucket'] else "{{ params['feeds_staging_aws_bucket_requester_pays_off'] }}"

            dataproc_label = str(client_record["client_name"]).replace("_", "-")

            DATAPROC_CLUSTER_NAME = f"pby-prdct-p-dpc-euwe1-stb-pll-{dataproc_label}"  # be mindful of dataproc cluster name length limit

            create_export_to_s3_cluster = BashOperator(
                task_id=f"create_export_to_s3_cluster_{client_record['client_name']}",
                bash_command=f"gcloud dataproc clusters create {DATAPROC_CLUSTER_NAME} "
                "--region {{ params['dataproc_region'] }} --master-machine-type n2-standard-2 --master-boot-disk-size 500 "
                "--num-workers 4 --worker-machine-type n1-standard-2 --worker-boot-disk-size 500 "
                "--image-version 1.5-debian10 "
                "--initialization-actions 'gs://{{ params['feeds_keys_gcs_bucket'] }}/pby-aws-send.sh' "
                "--project {{ var.value.env_project }} "
                "--max-age 5h --max-idle 15m "
                "--worker-boot-disk-type pd-ssd "
                "--num-worker-local-ssds 1 "
                "--worker-local-ssd-interface NVME "
            )

            # It clears the directory where the daily table is only so that it doesn't clear the monthly feed (visa versa)
            clear_s3 = S3DeleteObjectsOperator(
                task_id=f"clear_s3_{client_record['client_name']}",
                bucket=f"{destination_bucket}",
                prefix=f"{client_record['client_name']}/export_date={{{{ next_ds.replace('-', '') }}}}/{{{{ params['store_visits_daily_table'].replace('_','-') }}}}/",
                aws_conn_id="s3_conn",
            )

            # fmt: off
            source_uri = client_record["source_uri"]
            dest_uri = f"s3a://{destination_bucket}/{client_record['client_name']}/export_date={{{{ next_ds.replace('-', '') }}}}/{{{{ params['store_visits_daily_table'].replace('_','-') }}}}"
            # fmt: on

            submit_export_job = BashOperator(
                task_id=f"submit_export_job_{client_record['client_name']}",
                bash_command=f"gcloud dataproc jobs submit hadoop  "
                "--project={{ var.value.env_project }} "
                f"--region={{{{ params['dataproc_region'] }}}} --cluster={DATAPROC_CLUSTER_NAME} "
                "--jar file:///usr/lib/hadoop-mapreduce/hadoop-distcp.jar "
                f"-- -strategy dynamic -bandwidth 1000 -update {source_uri} {dest_uri}",
            )

            create_success_file = BashOperator(
                task_id=f"create_success_file_{client_record['client_name']}",
                bash_command="{% include './include/bash/pby_aws_pull/create_success.sh' %}",
                env={
                    "GCS_KEYS_BUCKET": f"{dag.params['feeds_keys_gcs_bucket']}",
                    "CLIENT_KEYFILE": "pby-aws-send",
                    "S3_STAGING_BUCKET": f"{destination_bucket}",
                    "S3_STAGING_BUCKET_PREFIX": f"{client_record['client_name']}/export_date={{{{ next_ds.replace('-', '') }}}}",
                },
            )

            (
                start
                >> create_export_to_s3_cluster
                >> clear_s3
                >> submit_export_job
                >> create_success_file
            )

        # man group are separate because they have their own bucket (execption not the rule)

        DATAPROC_CLUSTER_NAME_MAN_GROUP = f"pby-prdct-p-dpc-euwe1-stb-pll-man-group"

        create_export_to_s3_cluster_man_group = BashOperator(
            task_id="create_export_to_s3_cluster_man_group",
            bash_command=f"gcloud dataproc clusters create {DATAPROC_CLUSTER_NAME_MAN_GROUP} "
            f"--region {{{{ params['dataproc_region'] }}}} --master-machine-type n2-standard-4 --master-boot-disk-size 500 "
            "--num-workers 4 --worker-machine-type n2-standard-8 --worker-boot-disk-size 500 "
            "--image-version 1.5-debian10 "
            f"--initialization-actions 'gs://{{{{ params['feeds_keys_gcs_bucket']}}}}/pby-aws-send.sh' "
            f"--project {{{{ var.value.env_project }}}} "
            "--max-age 5h --max-idle 15m",
        )

        clear_s3_man_group = S3DeleteObjectsOperator(
            task_id="clear_s3_man_group",
            bucket="{{ params['feeds_staging_aws_bucket_man_group'] }}",
            prefix="export_date={{ next_ds.replace('-', '') }}/{{ params['store_visits_daily_table'].replace('_','-') }}/",
            aws_conn_id="s3_conn",
        )

        source_uri = "gs://{{ params['feeds_staging_gcs_daily_bucket'] }}/export_date={{ next_ds.replace('-', '') }}/type_1/{{ params['store_visits_daily_table'].replace('_','-') }}/historical_only/finance/csv_format"
        dest_uri = "s3a://{{ params['feeds_staging_aws_bucket_man_group'] }}/export_date={{ next_ds.replace('-', '') }}/{{ params['store_visits_daily_table'].replace('_','-') }}"

        submit_export_job_man_group = BashOperator(
            task_id="submit_export_job_man_group",
            bash_command=f"gcloud dataproc jobs submit hadoop  "
            f"--project={{{{ var.value.env_project }}}} "
            f"--region={dag.params['dataproc_region']} --cluster={DATAPROC_CLUSTER_NAME_MAN_GROUP} "
            "--jar file:///usr/lib/hadoop-mapreduce/hadoop-distcp.jar "
            f"-- -strategy dynamic -bandwidth 1000 -update {source_uri} {dest_uri}",
        )

        create_success_file_man_group = BashOperator(
            task_id="create_success_file_man_group",
            bash_command="{% include './include/bash/pby_aws_pull/create_success.sh' %}",
            env={
                "GCS_KEYS_BUCKET": f"{dag.params['feeds_keys_gcs_bucket']}",
                "CLIENT_KEYFILE": "pby-aws-send",
                "S3_STAGING_BUCKET": f"{dag.params['feeds_staging_aws_bucket_man_group']}",
                "S3_STAGING_BUCKET_PREFIX": f"export_date={{{{ next_ds.replace('-', '') }}}}",
            },
        )

        (
            start
            >> create_export_to_s3_cluster_man_group
            >> clear_s3_man_group
            >> submit_export_job_man_group
            >> create_success_file_man_group
        )

    return pby_aws_pull_group
