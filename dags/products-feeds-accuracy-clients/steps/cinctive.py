"""NO LONGER A CLIENT"""
# from __future__ import annotations

# import numpy as np
# from dateutil.relativedelta import relativedelta

# from airflow.operators.empty import EmptyOperator
# from airflow.models import TaskInstance, Variable
# from airflow.utils.task_group import TaskGroup
# from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
# from airflow.operators.bash import BashOperator
# from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
# from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator

# from common.operators.bigquery import OlvinBigQueryOperator
# from common.operators.exceptions import MarkSuccessOperator
# from common.utils import callbacks


# def register(dag, start: TaskInstance) -> TaskGroup:
#     """
#     Register tasks on the dag

#     Args:
#         start (airflow.models.TaskInstance): Task instance all tasks will
#         be registered downstream from.

#     Returns:
#         airflow.utils.task_group.TaskGroup: The task group in this section.
#     """
#     def branch_task_first_monday_fn(**context):
#         """
#         If the run date is the first Monday of the month, then return the task ID of the task that
#         clears the historical table. Otherwise, return the task ID of the task that clears the weekly
#         table.
#         :return: The name of the task to be executed.
#         """
#         run_date = context["next_ds"]
#         first_monday = Variable.get("first_monday")
#         return (
#             "cinctive.clear_gcs_placekeys_monthly"
#             if run_date == first_monday
#             else "cinctive.clear_gcs_store_visits_weekly"
#         )

        
#     with TaskGroup(group_id="cinctive") as group:

#         DATAPROC_CLUSTER_NAME=f"pby-product-p-dpc-euwe1-cinctive-s3"
#         create_export_to_s3_cluster = BashOperator(
#         task_id="create_export_to_s3_cluster",
#         bash_command=f"gcloud dataproc clusters create {DATAPROC_CLUSTER_NAME} "
#         f"--region {dag.params['dataproc_region']} --master-machine-type n2-standard-4 --master-boot-disk-size 500 "
#         "--num-workers 4 --worker-machine-type n2-standard-8 --worker-boot-disk-size 500 "
#         "--image-version 1.5-debian10 "
#         f"--initialization-actions 'gs://{dag.params['feeds_keys_gcs_bucket']}/pby-aws-send.sh' "
#         f"--project {{{{ var.value.env_project }}}} "
#         "--max-age 10h --max-idle 15m",
#         trigger_rule="none_failed_min_one_success"
#         )
#         end = EmptyOperator(task_id="end", depends_on_past=False,trigger_rule="all_done")

        

#         branch_task_first_monday = BranchPythonOperator(
#             task_id='branch_task_first_monday',
#             provide_context=True,
#             python_callable=branch_task_first_monday_fn,
#         )
        
#         start >> branch_task_first_monday

#         clear_gcs_placekeys_monthly = GCSDeleteObjectsOperator(
#             task_id="clear_gcs_placekeys_monthly",
#             bucket_name="{{ params['feeds_staging_gcs_no_pays_bucket'] }}",
#             prefix="cinctive/export_date={{ next_ds.replace('-', '') }}/{{ params['placekeys_table'] }}/",
#         )

#         clear_gcs_store_visits_monthly = GCSDeleteObjectsOperator(
#             task_id="clear_gcs_store_visits_monthly",
#             bucket_name="{{ params['feeds_staging_gcs_no_pays_bucket'] }}",
#             prefix="cinctive/export_date={{ next_ds.replace('-', '') }}/{{ params['store_visits_table'].replace('_','-') }}/",
#         )

#         clear_gcs_store_visits_weekly = GCSDeleteObjectsOperator(
#             task_id="clear_gcs_store_visits_weekly",
#             bucket_name="{{ params['feeds_staging_gcs_no_pays_bucket'] }}",
#             prefix="cinctive/export_date={{ next_ds.replace('-', '') }}/{{ params['store_visits_table'].replace('_','-') }}/",
#         )

#         clear_gcs_store_visitors_monthly = GCSDeleteObjectsOperator(
#             task_id="clear_gcs_store_visitors_monthly",
#             bucket_name="{{ params['feeds_staging_gcs_no_pays_bucket'] }}",
#             prefix="cinctive/export_date={{ next_ds.replace('-', '') }}/{{ params['store_visitors_table'].replace('_','-') }}/",
#         )

#         clear_gcs_store_visits_trend_monthly = GCSDeleteObjectsOperator(
#             task_id="clear_gcs_store_visits_trend_monthly",
#             bucket_name="{{ params['feeds_staging_gcs_no_pays_bucket'] }}",
#             prefix="cinctive/export_date={{ next_ds.replace('-', '') }}/{{ params['store_visits_trend_table'].replace('_','-') }}/",
#         )

#         export_placekeys_monthly = OlvinBigQueryOperator(
#             task_id="export_placekeys_monthly",
#             query="{% include './bigquery/cinctive/placekeys_monthly.sql' %}"
#         )

#         export_store_visits_monthly = OlvinBigQueryOperator(
#             task_id="export_store_visits_monthly",
#             query="{% include './bigquery/cinctive/store_visits_monthly.sql' %}",
#             billing_tier="high"
#         )

#         export_store_visits_weekly = OlvinBigQueryOperator(
#             task_id="export_store_visits_weekly",
#             query="{% include './bigquery/cinctive/store_visits_weekly.sql' %}"
#         )

#         export_store_visitors_monthly = OlvinBigQueryOperator(
#             task_id="export_store_visitors_monthly",
#             query="{% include './bigquery/cinctive/store_visitors_monthly.sql' %}",
#             billing_tier="high"
#         )

#         export_store_visits_trend_monthly = OlvinBigQueryOperator(
#             task_id="export_store_visits_trend_monthly",
#             query="{% include './bigquery/cinctive/store_visits_trend_monthly.sql' %}"
#         )

#         clear_s3 = S3DeleteObjectsOperator(
#             task_id="clear_s3",
#             bucket="{{ params['feeds_staging_aws_bucket'] }}",
#             prefix="cinctive/export_date={{ next_ds.replace('-', '') }}/",
#             aws_conn_id ="s3_conn",
#         )
#         submit_export_job = BashOperator(
#         task_id="submit_export_job",
#         bash_command=f"gcloud dataproc jobs submit hadoop  "
#         f"--project={{{{ var.value.env_project }}}} "
#         f"--region={dag.params['dataproc_region']} --cluster={DATAPROC_CLUSTER_NAME} "
#         "--jar file:///usr/lib/hadoop-mapreduce/hadoop-distcp.jar "
#         f"-- -update gs://{{{{ params['feeds_staging_gcs_no_pays_bucket'] }}}}/cinctive/export_date={{{{ next_ds.replace('-', '') }}}} s3a://{{{{ params['feeds_staging_aws_bucket'] }}}}/cinctive/export_date={{{{ next_ds.replace('-', '') }}}}",
#         )

#         push_to_client = BashOperator(
#             task_id="push_to_client",
#             bash_command="{% include './bash/cinctive/send.sh' %}",
#             env={
#                 "GCS_KEYS_BUCKET": f"{dag.params['feeds_keys_gcs_bucket']}",
#                 "CLIENT_KEYFILE": "client-aws-send",
#                 "S3_STAGING_BUCKET": f"{dag.params['feeds_staging_aws_bucket']}",
#                 "S3_STAGING_BUCKET_PREFIX": "cinctive",
#                 "S3_CLIENT_ACCESSPOINT_ARN": "arn:aws:s3:us-east-2:022591486057:accesspoint/cinctive-passby",
#                 "S3_CLIENT_ACCESSPOINT_ARN_PREFIX": "cinctive",
#             },
#         )

#         branch_task_first_monday >> clear_gcs_placekeys_monthly >> export_placekeys_monthly >> [clear_gcs_store_visits_monthly,clear_gcs_store_visitors_monthly,clear_gcs_store_visits_trend_monthly]
#         clear_gcs_store_visits_monthly >> export_store_visits_monthly
#         clear_gcs_store_visitors_monthly >> export_store_visitors_monthly
#         clear_gcs_store_visits_trend_monthly >> export_store_visits_trend_monthly
#         [export_store_visits_monthly,export_store_visitors_monthly,export_store_visits_trend_monthly] >> create_export_to_s3_cluster >> clear_s3 >> submit_export_job >> push_to_client >> end
#         branch_task_first_monday >> clear_gcs_store_visits_weekly >> export_store_visits_weekly >> create_export_to_s3_cluster >> clear_s3 >> submit_export_job >> push_to_client >> end

#     return group
