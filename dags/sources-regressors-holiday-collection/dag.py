import os
from datetime import datetime
from importlib.machinery import SourceFileLoader

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from common.utils import dag_args

path = f"{os.path.dirname(os.path.realpath(__file__))}"
DAG_ID = path.split("/")[-1]
steps_path = f"{path}/steps"

holidays_coll = SourceFileLoader(
    "holidays_coll", f"{steps_path}/holidays_coll.py"
).load_module()

env_args = dag_args.make_env_args(
    schedule_interval="0 0 1 6 *",
)

default_args = dag_args.make_default_args(
    start_date=datetime(2022, 9, 14),
    retries=0,
)

with DAG(
    DAG_ID,
    default_args=default_args,
    concurrency=12,
    schedule_interval=env_args["schedule_interval"],  # 1 June yearly
    tags=[env_args["env"], "yearly"],
    params=dag_args.load_config(scope="global"),
    doc_md=dag_args.load_docs(__file__),
) as dag:

    start = EmptyOperator(task_id="start")

    holidays_coll_end = holidays_coll.register(start=start, dag=dag)
