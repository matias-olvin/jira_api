import os
from datetime import datetime
from importlib.machinery import SourceFileLoader

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from common.utils import callbacks, dag_args, slack_users

folder_path = f"{os.path.dirname(os.path.realpath(__file__))}"
DAG_ID = folder_path.split("/")[-1]

sample_general = SourceFileLoader(
    "sample_general", f"{folder_path}/steps/sample_general.py"
).load_module()

sample_finance = SourceFileLoader(
    "sample_finance", f"{folder_path}/steps/sample_finance.py"
).load_module()

env_args = dag_args.make_env_args(schedule_interval="0 6 1 * *")
default_args = dag_args.make_default_args(
    start_date=datetime(2022, 9, 16),
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

    sample_general_end = sample_general.register(start=start)

    sample_finance_end = sample_finance.register(start=start)

    [sample_general_end, sample_finance_end] >> end
