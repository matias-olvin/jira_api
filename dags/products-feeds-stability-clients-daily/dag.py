import os
from importlib.machinery import SourceFileLoader

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from common.utils import callbacks, dag_args, slack_users

folder_path = f"{os.path.dirname(os.path.realpath(__file__))}"
DAG_ID = folder_path.split("/")[-1]

common = SourceFileLoader("common", f"{folder_path}/steps/common.py").load_module()

pby_aws_push = SourceFileLoader(
    "pby_aws_push", f"{folder_path}/steps/pby_aws_push.py"
).load_module()

env_args = dag_args.make_env_args(schedule_interval="0 11 * * *")
default_args = dag_args.make_default_args(
    start_date=pendulum.datetime(2022, 9, 16),
    depends_on_past=True,
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
    end = EmptyOperator(task_id="end")

    common_end = common.register(dag=dag, start=start)

    pby_aws_push_end = pby_aws_push.register(start=common_end, dag=dag)

    [pby_aws_push_end] >> end
