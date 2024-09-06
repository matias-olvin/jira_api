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
    DATAPROC_CLUSTER_NAME=f"pby-product-p-dpc-euwe1-type2-s3-export"

    create_export_to_s3_cluster_type2 = BashOperator(
    task_id="create_export_to_s3_cluster_type2",
    bash_command=f"gcloud dataproc clusters create {DATAPROC_CLUSTER_NAME} "
    f"--region {dag.params['dataproc_region']} --master-machine-type n2-standard-4 --master-boot-disk-size 500 "
    "--num-workers 4 --worker-machine-type n2-standard-8 --worker-boot-disk-size 500 "
    "--image-version 1.5-debian10 "
    "--initialization-actions 'gs://pby-product-p-gcs-euwe1-datafeed-keys/pby-aws-send.sh' "
    f"--project {{{{ var.value.env_project }}}} "
    "--max-age 10h --max-idle 15m",
    )

    sensor = ExternalTaskSensor(
            task_id="type2_placekey_sensor",
            external_dag_id="{{ params['placekeys_stability_feed'] }}",
            external_task_id="end",
            execution_date_fn=lambda dt: dt.replace(
            day=1,
            hour=6,
            ),
            mode="reschedule",
            poke_interval=60 * 5,
            timeout=60 * 60 * 24,
        )

    with TaskGroup(group_id="finance-type2") as finance_type2:

        

        clear_s3_placekeys = S3DeleteObjectsOperator(
            task_id="clear_s3_placekeys",
            bucket="{{ params['data_feeds_s3_bucket'] }}",
            prefix="finance/type-2/{{ params['placekeys_table'].replace('_','-') }}/",
            aws_conn_id ="s3_conn",
        )

        clear_s3_store_visits = S3DeleteObjectsOperator(
            task_id="clear_s3_store_visits",
            bucket="{{ params['data_feeds_s3_bucket'] }}",
            prefix="finance/type-2/{{ params['store_visits_table'].replace('_','-') }}/",
            aws_conn_id ="s3_conn",
        )

        clear_s3_store_visitors = S3DeleteObjectsOperator(
            task_id="clear_s3_store_visitors",
            bucket="{{ params['data_feeds_s3_bucket'] }}",
            prefix="finance/type-2/{{ params['store_visitors_table'].replace('_','-') }}/",
            aws_conn_id ="s3_conn",
        )

        clear_s3_store_visits_trend = S3DeleteObjectsOperator(
            task_id="clear_s3_store_visits_trend",
            bucket="{{ params['data_feeds_s3_bucket'] }}",
            prefix="finance/type-2/{{ params['store_visits_trend_table'].replace('_','-') }}/",
            aws_conn_id ="s3_conn",
        )


        

        submit_export_placekeys_job = BashOperator(
        task_id="submit_export_placekeys_job",
        bash_command=f"gcloud dataproc jobs submit hadoop  "
        f"--project={{{{ var.value.env_project }}}} "
        f"--region={dag.params['dataproc_region']} --cluster={DATAPROC_CLUSTER_NAME} "
        "--jar file:///usr/lib/hadoop-mapreduce/hadoop-distcp.jar "
        f"-- -strategy dynamic -bandwidth 1000 -update gs://{{{{ params['public_feeds_type_2_finance_bucket'] }}}}/{{{{ params['placekeys_table'] }}}}/ s3a://{{{{ params['data_feeds_s3_bucket'] }}}}/finance/type-2/{{{{ params['placekeys_table'] }}}}/",
        )

        submit_export_store_visits_job = BashOperator(
        task_id="submit_export_store_visits_job",
        bash_command=f"gcloud dataproc jobs submit hadoop  "
        f"--project={{{{ var.value.env_project }}}} "
        f"--region={dag.params['dataproc_region']} --cluster={DATAPROC_CLUSTER_NAME} "
        "--jar file:///usr/lib/hadoop-mapreduce/hadoop-distcp.jar "
        f"-- -strategy dynamic -bandwidth 1000 -update gs://{{{{ params['public_feeds_type_2_finance_bucket'] }}}}/{{{{ params['store_visits_table'].replace('_','-') }}}}/ s3a://{{{{ params['data_feeds_s3_bucket'] }}}}/finance/type-2/{{{{ params['store_visits_table'].replace('_','-') }}}}/",
        )

        submit_export_store_visitors_job = BashOperator(
        task_id="submit_export_store_visitors_job",
        bash_command=f"gcloud dataproc jobs submit hadoop  "
        f"--project={{{{ var.value.env_project }}}} "
        f"--region={dag.params['dataproc_region']} --cluster={DATAPROC_CLUSTER_NAME} "
        "--jar file:///usr/lib/hadoop-mapreduce/hadoop-distcp.jar "
        f"-- -strategy dynamic -bandwidth 1000 -update gs://{{{{ params['public_feeds_type_2_finance_bucket'] }}}}/{{{{ params['store_visitors_table'].replace('_','-') }}}}/ s3a://{{{{ params['data_feeds_s3_bucket'] }}}}/finance/type-2/{{{{ params['store_visitors_table'].replace('_','-') }}}}/",
        )

        submit_export_store_visits_trend_job = BashOperator(
        task_id="submit_export_store_visits_trend_job",
        bash_command=f"gcloud dataproc jobs submit hadoop  "
        f"--project={{{{ var.value.env_project }}}} "
        f"--region={dag.params['dataproc_region']} --cluster={DATAPROC_CLUSTER_NAME} "
        "--jar file:///usr/lib/hadoop-mapreduce/hadoop-distcp.jar "
        f"-- -strategy dynamic -bandwidth 1000 -update gs://{{{{ params['public_feeds_type_2_finance_bucket'] }}}}/{{{{ params['store_visits_trend_table'].replace('_','-') }}}}/ s3a://{{{{ params['data_feeds_s3_bucket'] }}}}/finance/type-2/{{{{ params['store_visits_trend_table'].replace('_','-') }}}}/",
        )

        clear_s3_placekeys >> [clear_s3_store_visits,clear_s3_store_visitors,clear_s3_store_visits_trend]
        [clear_s3_store_visits,clear_s3_store_visitors,clear_s3_store_visits_trend] >> submit_export_placekeys_job
        submit_export_placekeys_job >> submit_export_store_visits_job >> submit_export_store_visitors_job >> submit_export_store_visits_trend_job

    with TaskGroup(group_id="general-type2") as general_type2:


        clear_s3_placekeys = S3DeleteObjectsOperator(
            task_id="clear_s3_placekeys",
            bucket="{{ params['data_feeds_s3_bucket'] }}",
            prefix="general/type-2/{{ params['placekeys_table'].replace('_','-') }}/",
            aws_conn_id ="s3_conn",
        )

        clear_s3_store_visits = S3DeleteObjectsOperator(
            task_id="clear_s3_store_visits",
            bucket="{{ params['data_feeds_s3_bucket'] }}",
            prefix="general/type-2/{{ params['store_visits_table'].replace('_','-') }}/",
            aws_conn_id ="s3_conn",
        )

        clear_s3_store_visitors = S3DeleteObjectsOperator(
            task_id="clear_s3_store_visitors",
            bucket="{{ params['data_feeds_s3_bucket'] }}",
            prefix="general/type-2/{{ params['store_visitors_table'].replace('_','-') }}/",
            aws_conn_id ="s3_conn",
        )

        clear_s3_store_visits_trend = S3DeleteObjectsOperator(
            task_id="clear_s3_store_visits_trend",
            bucket="{{ params['data_feeds_s3_bucket'] }}",
            prefix="general/type-2/{{ params['store_visits_trend_table'].replace('_','-') }}/",
            aws_conn_id ="s3_conn",
        )


        

        submit_export_placekeys_job = BashOperator(
        task_id="submit_export_placekeys_job",
        bash_command=f"gcloud dataproc jobs submit hadoop  "
        f"--project={{{{ var.value.env_project }}}} "
        f"--region={dag.params['dataproc_region']} --cluster={DATAPROC_CLUSTER_NAME} "
        "--jar file:///usr/lib/hadoop-mapreduce/hadoop-distcp.jar "
        f"-- -strategy dynamic -bandwidth 1000 -update gs://{{{{ params['public_feeds_type_2_bucket'] }}}}/{{{{ params['placekeys_table'] }}}}/ s3a://{{{{ params['data_feeds_s3_bucket'] }}}}/general/type-2/{{{{ params['placekeys_table'] }}}}/",
        )

        submit_export_store_visits_job = BashOperator(
        task_id="submit_export_store_visits_job",
        bash_command=f"gcloud dataproc jobs submit hadoop  "
        f"--project={{{{ var.value.env_project }}}} "
        f"--region={dag.params['dataproc_region']} --cluster={DATAPROC_CLUSTER_NAME} "
        "--jar file:///usr/lib/hadoop-mapreduce/hadoop-distcp.jar "
        f"-- -strategy dynamic -bandwidth 1000 -update gs://{{{{ params['public_feeds_type_2_bucket'] }}}}/{{{{ params['store_visits_table'].replace('_','-') }}}}/ s3a://{{{{ params['data_feeds_s3_bucket'] }}}}/general/type-2/{{{{ params['store_visits_table'].replace('_','-') }}}}/",
        )

        submit_export_store_visitors_job = BashOperator(
        task_id="submit_export_store_visitors_job",
        bash_command=f"gcloud dataproc jobs submit hadoop  "
        f"--project={{{{ var.value.env_project }}}} "
        f"--region={dag.params['dataproc_region']} --cluster={DATAPROC_CLUSTER_NAME} "
        "--jar file:///usr/lib/hadoop-mapreduce/hadoop-distcp.jar "
        f"-- -strategy dynamic -bandwidth 1000 -update gs://{{{{ params['public_feeds_type_2_bucket'] }}}}/{{{{ params['store_visitors_table'].replace('_','-') }}}}/ s3a://{{{{ params['data_feeds_s3_bucket'] }}}}/general/type-2/{{{{ params['store_visitors_table'].replace('_','-') }}}}/",
        )

        submit_export_store_visits_trend_job = BashOperator(
        task_id="submit_export_store_visits_trend_job",
        bash_command=f"gcloud dataproc jobs submit hadoop  "
        f"--project={{{{ var.value.env_project }}}} "
        f"--region={dag.params['dataproc_region']} --cluster={DATAPROC_CLUSTER_NAME} "
        "--jar file:///usr/lib/hadoop-mapreduce/hadoop-distcp.jar "
        f"-- -strategy dynamic -bandwidth 1000 -update gs://{{{{ params['public_feeds_type_2_bucket'] }}}}/{{{{ params['store_visits_trend_table'].replace('_','-') }}}}/ s3a://{{{{ params['data_feeds_s3_bucket'] }}}}/general/type-2/{{{{ params['store_visits_trend_table'].replace('_','-') }}}}/",
        )

        clear_s3_placekeys >> [clear_s3_store_visits,clear_s3_store_visitors,clear_s3_store_visits_trend]
        [clear_s3_store_visits,clear_s3_store_visitors,clear_s3_store_visits_trend] >> submit_export_placekeys_job
        submit_export_placekeys_job >> submit_export_store_visits_job >> submit_export_store_visitors_job >> submit_export_store_visits_trend_job
    
    delete_cluster = DataprocDeleteClusterOperator(
    task_id="delete_cluster_type2",
    project_id=Variable.get("env_project"),
    cluster_name=DATAPROC_CLUSTER_NAME,
    region=dag.params["dataproc_region"],
    dag=dag,
)
    
    start >> sensor >> create_export_to_s3_cluster_type2 >> general_type2 >> finance_type2 >> delete_cluster

    return delete_cluster