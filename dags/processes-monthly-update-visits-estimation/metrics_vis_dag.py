"DAG ID: visits_estimation_gtvm_metrics"

import os, pendulum
from datetime import datetime
from importlib.machinery import SourceFileLoader
from common.utils import dag_args, callbacks

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from common.utils import slack_users

path = f"{os.path.dirname(os.path.realpath(__file__))}"
steps_path = path + "/steps"

DAG_ID = path.split("/")[-1]

env_args = dag_args.make_env_args(
    dag_id=DAG_ID,
)

vis_steps = SourceFileLoader("vis_steps", f"{steps_path}/trigger_vis.py").load_module()

with DAG(
    env_args["dag_id"]+"-visualizations",
    default_args=dag_args.make_default_args(
    start_date=pendulum.datetime(2022, 8, 16, tz="Europe/London"),
    retries=0,
    on_failure_callback=callbacks.task_fail_slack_alert(slack_users.MATIAS, slack_users.IGNACIO),
),
    concurrency=12,
    schedule_interval=env_args["schedule_interval"],
    tags=[env_args["env"], "monthly"],
    params=dag_args.load_config(__file__),
    doc_md=dag_args.load_docs(__file__),
) as dag:
    start = DummyOperator(task_id="start")
    vis_steps_end = vis_steps.register(dag, start)
    end = DummyOperator(task_id="end")
    vis_steps_end >> end