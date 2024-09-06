import os
from datetime import datetime
from importlib.machinery import SourceFileLoader

from airflow import DAG
from airflow.operators.empty import EmptyOperator

from common.utils import dag_args, callbacks

path = f"{os.path.dirname(os.path.realpath(__file__))}"
steps_path = path + "/steps"

DAG_ID = path.split("/")[-1]

ftp_steps = SourceFileLoader("ftp_steps", f"{steps_path}/ftp_steps.py").load_module()

env_args = dag_args.make_env_args(
    dag_id=DAG_ID,
    schedule_interval="0 3 * * *",
)
default_args = dag_args.make_default_args(
    start_date=datetime(2021, 4, 5),
    retries=3,
    email="kartikey@olvin.com",
    email_on_failure=True,
    email_on_retry=False,
    on_failure_callback=callbacks.task_fail_slack_alert(
        "U02KJ556S1H",
        channel=env_args["env"]
    ),
)

with DAG(
    env_args["dag_id"],
    default_args=default_args,
    concurrency=4,
    schedule_interval=env_args["schedule_interval"],
    tags=[env_args["env"], "daily", "export"],
    params=dag_args.load_config(scope="global"),
    doc_md=dag_args.load_docs(__file__),
) as dag:
    start = EmptyOperator(task_id="start")
    ftp_steps_end = ftp_steps.register(start, dag)
