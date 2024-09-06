import os
from importlib.machinery import SourceFileLoader

import airflow
import pendulum  # use pendulum for tz.
from airflow.operators.dummy import DummyOperator
from common.utils import callbacks, dag_args

path = f"{os.path.dirname(os.path.realpath(__file__))}"  # Get path to dag folder.
DAG_ID = path.split("/")[-1]

tasks = SourceFileLoader("tasks", f"{path}/steps/dynamic_tasks.py").load_module()

env_args = dag_args.make_env_args(
    schedule_interval="00 08 * * *",  # At 08:00. - https://crontab.guru
)
default_args = dag_args.make_default_args(
    start_date=pendulum.datetime(2023, 1, 1, tz="Europe/London"),
    retries=2,
    on_failure_callback=callbacks.task_fail_slack_alert(
        "#A012BCDEF3H", "#Z987YXWVU6T", channel=env_args["env"]  # User 1  # User 2
    ),  # Add one or more slack id.
)

with airflow.DAG(
    DAG_ID,
    default_args=default_args,
    description="Example of DAG best practices.",
    schedule_interval=env_args["schedule_interval"],
    tags=[env_args["env"], "none", "example"],  # add tags relevant to the DAG.
    doc_md=dag_args.load_docs(__file__),  # Load README.md to DAG.
) as dag:
    start = DummyOperator(task_id="start")

    task_end = tasks.register(start=start, dag=dag, year=1, month=1)
