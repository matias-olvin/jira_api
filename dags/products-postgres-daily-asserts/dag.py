import os
from importlib.machinery import SourceFileLoader

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from common.utils import callbacks, dag_args

path = f"{os.path.dirname(os.path.realpath(__file__))}"
DAG_ID = path.split("/")[-1]
steps_path = f"{path}/steps"

naics_code_check = SourceFileLoader(
    "naics_code_check", f"{steps_path}/naics_code_check.py"
).load_module()

env_args = dag_args.make_env_args(schedule_interval="0 10 * * *")

default_args = dag_args.make_default_args(
    start_date=pendulum.datetime(2022, 8, 16, tz="Europe/London"),
    retries=0,
)

with DAG(
    DAG_ID,
    params=dag_args.load_config(scope="global"),
    default_args=default_args,
    schedule_interval=env_args["schedule_interval"],
    tags=[env_args["env"], "scheduled"],
    doc_md=dag_args.load_docs(__file__),
    on_failure_callback=callbacks.task_fail_slack_alert(
        "U02KJ556S1H", channel=env_args["env"]  # Kartikey
    ),
) as dag:
    start = EmptyOperator(task_id="start")

    naics_code_check_end = naics_code_check.register(start=start)
