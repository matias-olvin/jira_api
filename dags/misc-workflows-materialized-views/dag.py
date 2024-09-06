"""
DAG ID: model_gtvm
"""
import os, pendulum
from datetime import datetime
from importlib.machinery import SourceFileLoader
from common.utils import dag_args, callbacks
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

path = f"{os.path.dirname(os.path.realpath(__file__))}"
steps_path = path + "/steps/"
olvin_views = SourceFileLoader(
    "olvin_views", steps_path + "olvin_views.py"
).load_module()

DAG_ID = path.split("/")[-1]

env_args = dag_args.make_env_args(
    dag_id=DAG_ID,
)

with DAG(
    env_args["dag_id"],
    default_args=dag_args.make_default_args(
    depends_on_past=True,
    start_date=pendulum.datetime(2021, 4, 2, tz="Europe/London"),
    retries=0,
    email="kartikey@olvin.com",
    wait_for_downstream=True,
    on_failure_callback=callbacks.task_fail_slack_alert("U02KJ556S1H"),
),
    schedule_interval=env_args["schedule_interval"],
    tags=[env_args["env"], "misc"],
    params=dag_args.load_config(__file__),
    doc_md=dag_args.load_docs(__file__),
) as dag:
    start = DummyOperator(task_id="start", dag=dag)
    olvin_views_end = olvin_views.register(dag=dag, start=start)
