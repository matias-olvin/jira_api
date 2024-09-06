"""
DAG ID: poi_visits_backfill
"""
import os
from datetime import datetime
from importlib.machinery import SourceFileLoader

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from common.utils import callbacks, dag_args, slack_users

path = f"{os.path.dirname(os.path.realpath(__file__))}"
DAG_ID = path.split("/")[-1]

poi_visits = SourceFileLoader("poi_visits", f"{path}/steps/poi_visits.py").load_module()

env_args = dag_args.make_env_args()
default_args = dag_args.make_default_args(
    start_date=datetime(2022, 10, 1),
    retries=3,
)

with DAG(
    DAG_ID,
    default_args=default_args,
    schedule_interval=None,
    tags=["prod", "smc", "backfill"],
    params=dag_args.load_config(__file__),
    doc_md=dag_args.load_docs(__file__),
    on_failure_callback=callbacks.task_fail_slack_alert(
        slack_users.MATIAS,
        channel=env_args["env"],
    ),
) as dag:
    start = DummyOperator(task_id="start")
    poi_visits_end = poi_visits.register(dag, start)
