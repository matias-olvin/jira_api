import os
from importlib.machinery import SourceFileLoader

import airflow
import pendulum
from airflow.operators.empty import EmptyOperator
from common.utils import callbacks, dag_args

path = f"{os.path.dirname(os.path.realpath(__file__))}"  # Get path to dag folder.
DAG_ID = path.split("/")[-1]

client_send = SourceFileLoader("tasks", f"{path}/steps/client_send.py").load_module()

env_args = dag_args.make_env_args()

default_args = dag_args.make_default_args(
    start_date=pendulum.datetime(2023, 1, 1, tz="Europe/London"),
    retries=2,
)

with airflow.DAG(
    DAG_ID,
    default_args=default_args,
    description="For sending large amounts of data to client aws buckets.",
    schedule_interval=env_args["schedule_interval"],
    tags=[env_args["env"], "misc"],  # add tags relevant to the DAG.
) as dag:
    start = EmptyOperator(task_id="start")

    client_send_end = client_send.register(start, dag)
