"""
DAG ID: regressors_holidays
"""
import os
from datetime import datetime
from importlib.machinery import SourceFileLoader

# airflow imports
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

# other imports
from common.utils import dag_args

path = f"{os.path.dirname(os.path.realpath(__file__))}"
DAG_ID = path.split("/")[-1]

steps_path = f"{path}/steps/"
holidays_dict = SourceFileLoader(
    "holidays_dict", steps_path + "holidays_dict.py"
).load_module()

env_args = dag_args.make_env_args(
    dag_id=DAG_ID,
    schedule_interval=None,
)

with DAG(
    env_args["dag_id"],
    default_args=dag_args.make_default_args(
        start_date=datetime(2022, 9, 14),
        retries=0,
    ),
    concurrency=12,
    schedule_interval=env_args["schedule_interval"],  # "0 0 8 * *",
    tags=[env_args["env"], "misc"],
    params=dag_args.load_config(__file__),
    doc_md=dag_args.load_docs(__file__),
) as dag:
    start = DummyOperator(task_id="start", dag=dag)
    dictionary_end = holidays_dict.register(dag, start)
    end = DummyOperator(task_id="end", dag=dag)
    dictionary_end >> end
