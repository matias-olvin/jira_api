import os
from importlib.machinery import SourceFileLoader

import airflow
import pendulum  # use pendulum for tz.
from airflow.operators.dummy import DummyOperator
from common.utils import dag_args

path = f"{os.path.dirname(os.path.realpath(__file__))}"  # Get path to dag folder.
DAG_ID = path.split("/")[-1]

trigger_dag = SourceFileLoader(
    "trigger_dag", f"{path}/steps/trigger_dag.py"
).load_module()

env_args = dag_args.make_env_args(
    schedule_interval=None,
)
default_args = dag_args.make_default_args(
    start_date=pendulum.datetime(2023, 1, 1, tz="Europe/London"),
    retries=2,
)

with airflow.DAG(
    DAG_ID + "-parent",
    default_args=default_args,
    description="Example of DAG best practices.",
    schedule_interval=env_args["schedule_interval"],
    tags=[
        env_args["env"],
        "None",
        "example",
        "trigger",
    ],  # add tags relevant to the DAG.
) as dag:
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")  # Task for Sensor.

    trigger_dag_end = trigger_dag.register(start, dag)
    trigger_dag_end >> end
