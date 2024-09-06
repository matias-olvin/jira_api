import os
from datetime import datetime
from importlib.machinery import SourceFileLoader

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from common.utils import dag_args

path = f"{os.path.dirname(os.path.realpath(__file__))}"
DAG_ID = path.split("/")[-1]
steps_path = f"{path}/steps"

holidays_dict = SourceFileLoader(
    "holidays_dict", f"{steps_path}/holidays_dict.py"
).load_module()

env_args = dag_args.make_env_args(
    schedule_interval=None,
)

default_args = dag_args.make_default_args(
    start_date=datetime(2022, 9, 14),
    retries=0,
)

with DAG(
    DAG_ID,
    default_args=default_args,
    concurrency=12,
    schedule_interval=env_args["schedule_interval"],  # "0 0 8 * *",
    tags=[env_args["env"], "misc"],
    params=dag_args.load_config(scope="global"),
    doc_md=dag_args.load_docs(__file__),
) as dag:

    start = EmptyOperator(task_id="start")

    dictionary_end = holidays_dict.register(start=start, dag=dag)

    end = EmptyOperator(task_id="end")

    dictionary_end >> end
