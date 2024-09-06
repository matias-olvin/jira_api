import os
from importlib.machinery import SourceFileLoader

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from common.utils import callbacks, dag_args

path = f"{os.path.dirname(os.path.realpath(__file__))}"
DAG_ID = path.split("/")[-1]
steps_path = f"{path}/steps"

queries = SourceFileLoader(
    "queries", f"{steps_path}/queries.py"
).load_module()

env_args = dag_args.make_env_args(schedule_interval="0 9 * * 1")

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
        "U05FN3F961X", channel=env_args["env"]  # Ignacio
    ),
) as dag:
    start = EmptyOperator(task_id="start")

    queries_end = queries.register(start=start)
