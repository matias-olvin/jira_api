import os
from datetime import datetime
from importlib.machinery import SourceFileLoader

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from common.utils import dag_args, callbacks

path = f"{os.path.dirname(os.path.realpath(__file__))}"
DAG_ID = path.split("/")[-1]

metrics = SourceFileLoader("metrics", f"{path}/steps/metrics.py").load_module()

env_args = dag_args.make_env_args()
default_args = dag_args.make_default_args(
    start_date=datetime(2022, 1, 1),
    retries=0,
)

with DAG(
    DAG_ID,
    default_args=default_args,
    schedule_interval=None,
    tags=["prod", "smc"],
    params=dag_args.load_config(__file__),
    doc_md=dag_args.load_docs(__file__),
    on_failure_callback=callbacks.task_fail_slack_alert(
        "U034YDXAD1R",  # Jake
    ),
) as metrics_dag:
    start = DummyOperator(task_id="start")
    metrics_end = metrics.register(metrics_dag, start)
