import os
from datetime import datetime, timedelta
from importlib.machinery import SourceFileLoader

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from common.utils import dag_args

path = f"{os.path.dirname(os.path.realpath(__file__))}"
DAG_ID = path.split("/")[-1]
steps_path = f"{path}/steps"

group_visits_dict = SourceFileLoader(
    "group_visits_dict", f"{steps_path}/group_visits_dict.py"
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
    schedule_interval=env_args["schedule_interval"],
    tags=[env_args["env"], "monthly"],
    dagrun_timeout=timedelta(hours=12),
    params=dag_args.load_config(scope="global"),
    doc_md=dag_args.load_docs(__file__),
) as dag:
    start = EmptyOperator(task_id="start")

    group_visits_dict_end = group_visits_dict.register(start=start, dag=dag)

    end = EmptyOperator(task_id="end")

    group_visits_dict_end >> end
