import os
from datetime import datetime
from importlib.machinery import SourceFileLoader

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from common.utils import callbacks, dag_args

path = f"{os.path.dirname(os.path.realpath(__file__))}"
DAG_ID = path.split("/")[-1]

dictionary_tasks = SourceFileLoader(
    "dictionary-tasks", f"{path}/steps/dictionary-tasks.py"
).load_module()

env_args = dag_args.make_env_args(
    dag_id=DAG_ID,
)

default_args = dag_args.make_default_args(
    start_date=datetime(2022, 9, 14),
    retries=0,
    on_failure_callback=callbacks.task_fail_slack_alert(
        "U05M60N8DMX", channel=env_args["env"]  # Matias
    ),
)

with DAG(
    DAG_ID,
    default_args=default_args,
    concurrency=12,
    schedule_interval=env_args["schedule_interval"],
    params=dag_args.load_config(scope="global"),
    doc_md=dag_args.load_docs(__file__),
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    dictionary_tasks_end = dictionary_tasks.register(start)
    dictionary_tasks_end >> end
