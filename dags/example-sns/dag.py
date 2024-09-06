import os
from importlib.machinery import SourceFileLoader

import airflow
import pendulum  # use pendulum for tz.
from airflow.operators.empty import EmptyOperator
from common.utils import dag_args

path = f"{os.path.dirname(os.path.realpath(__file__))}"  # Get path to dag folder.
DAG_ID = path.split("/")[-1]

copy_to_sns = SourceFileLoader(
    "copy_to_sns", f"{path}/steps/copy_to_sns.py"
).load_module()
sns_steps = SourceFileLoader("sns_steps", f"{path}/steps/sns_steps.py").load_module()
copy_to_olvin = SourceFileLoader(
    "copy_to_olvin", f"{path}/steps/copy_to_olvin.py"
).load_module()

env_args = dag_args.make_env_args()
default_args = dag_args.make_default_args(
    start_date=pendulum.datetime(2023, 1, 1, tz="Europe/London"),
    retries=2,
)

with airflow.DAG(
    DAG_ID,
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
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    copy_to_sns_end = copy_to_sns.register(start, dag)
    sns_steps_end = sns_steps.register(copy_to_sns_end, dag)
    copy_to_olvin_end = copy_to_olvin.register(sns_steps_end, dag)
    copy_to_olvin_end >> end
