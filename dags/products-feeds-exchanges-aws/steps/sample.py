from __future__ import annotations

from datetime import datetime
from dateutil.relativedelta import relativedelta
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
from airflow.models import TaskInstance, Variable

from airflow.operators.bash import BashOperator

from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator

from airflow.providers.google.cloud.operators.dataproc import (
    DataprocDeleteClusterOperator,
)

def register(dag, start: TaskInstance) -> TaskGroup:
    """
    Register tasks on the dag

    Args:
        start (airflow.models.TaskInstance): Task instance all tasks will
        be registered downstream from.

    Returns:
        airflow.utils.task_group.TaskGroup: The task group in this section.
    """
    DATAPROC_CLUSTER_NAME=f"pby-product-p-dpc-euwe1-sample-s3-export"
    create_export_to_s3_cluster_sample = BashOperator(
    task_id="create_export_to_s3_cluster_sample",
    bash_command=f"gcloud dataproc clusters create {DATAPROC_CLUSTER_NAME} "
    f"--region {dag.params['dataproc_region']} --master-machine-type n2-standard-4 --master-boot-disk-size 500 "
    "--num-workers 4 --worker-machine-type n2-standard-8 --worker-boot-disk-size 500 "
    "--image-version 1.5-debian10 "
    "--initialization-actions 'gs://pby-product-p-gcs-euwe1-datafeed-keys/pby-aws-send.sh' "
    f"--project {{{{ var.value.env_project }}}} "
    "--max-age 2h --max-idle 1h",
    )
    sensor = ExternalTaskSensor(
            task_id="sample_placekey_sensor",
            external_dag_id="{{ params['sample_feed'] }}",
            external_task_id="end",
            execution_date_fn=lambda dt: dt.replace(
            day=1,
            hour=6,
            ),
            mode="reschedule",
            poke_interval=60 * 5,
            timeout=60 * 60 * 24,
        )
    
    start >> sensor >> create_export_to_s3_cluster_sample

    with TaskGroup(group_id="general-sample") as general_sample:

        


        clear_s3_sample = S3DeleteObjectsOperator(
            task_id="clear_s3_sample",
            bucket="{{ params['data_feeds_s3_bucket'] }}",
            prefix="general/sample/",
            aws_conn_id ="s3_conn",
        )


        submit_export_sample_job = BashOperator(
        task_id="submit_export_sample_job",
        bash_command=f"gcloud dataproc jobs submit hadoop  "
        f"--project={{{{ var.value.env_project }}}} "
        f"--region={dag.params['dataproc_region']} --cluster={DATAPROC_CLUSTER_NAME} "
        "--jar file:///usr/lib/hadoop-mapreduce/hadoop-distcp.jar "
        f"-- -strategy dynamic -bandwidth 1000 -update gs://{{{{ params['sample_feeds_bucket'] }}}}/general s3a://{{{{ params['data_feeds_s3_bucket'] }}}}/general/sample/",
        )

        clear_s3_sample >> submit_export_sample_job

    with TaskGroup(group_id="finance-sample") as finance_sample:


        clear_s3_sample = S3DeleteObjectsOperator(
            task_id="clear_s3_sample",
            bucket="{{ params['data_feeds_s3_bucket'] }}",
            prefix="finance/sample/",
            aws_conn_id ="s3_conn",
        )


        submit_export_sample_job = BashOperator(
        task_id="submit_export_sample_job",
        bash_command=f"gcloud dataproc jobs submit hadoop  "
        f"--project={{{{ var.value.env_project }}}} "
        f"--region={dag.params['dataproc_region']} --cluster={DATAPROC_CLUSTER_NAME} "
        "--jar file:///usr/lib/hadoop-mapreduce/hadoop-distcp.jar "
        f"-- -strategy dynamic -bandwidth 1000 -update gs://{{{{ params['sample_feeds_bucket'] }}}}/finance s3a://{{{{ params['data_feeds_s3_bucket'] }}}}/finance/sample/",
        )

        clear_s3_sample >> submit_export_sample_job
    
    delete_cluster = DataprocDeleteClusterOperator(
    task_id="delete_cluster_sample",
    project_id=Variable.get("env_project"),
    cluster_name=DATAPROC_CLUSTER_NAME,
    region=dag.params["dataproc_region"],
    dag=dag,
)
    create_export_to_s3_cluster_sample >> general_sample >> finance_sample >> delete_cluster

    return delete_cluster