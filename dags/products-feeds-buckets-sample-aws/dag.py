import os
from importlib.machinery import SourceFileLoader

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from common.utils import callbacks, dag_args, slack_users

folder_path = f"{os.path.dirname(os.path.realpath(__file__))}"
DAG_ID = folder_path.split("/")[-1]

# aws = SourceFileLoader("aws", f"{folder_path}/steps/aws.py").load_module()
aws = SourceFileLoader("aws", f"{folder_path}/steps/copy_s3_to_s3.py").load_module()

env_args = dag_args.make_env_args()

default_args = dag_args.make_default_args(
    start_date=pendulum.datetime(2022, 9, 16, tz="Europe/London"),
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
    start = EmptyOperator(task_id="start")

    sample_bucket_name = dag.params["requester_pays_sample_bucket"]

    aws_end = aws.register(start, dag, sample_bucket_name)

    end = EmptyOperator(
        task_id="end",
        on_success_callback=callbacks.task_end_custom_slack_alert(
            slack_users.MATIAS,
            channel=env_args["env"],
            msg_title=f"AWS Data Feeds Sample Available In Bucket: {sample_bucket_name}",
        ),
    )

    aws_end >> end
