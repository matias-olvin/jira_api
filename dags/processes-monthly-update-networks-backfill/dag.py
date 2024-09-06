"""
DAG ID: networks_pipeline
"""
import os, pendulum
from datetime import datetime
from importlib.machinery import SourceFileLoader
from common.utils import dag_args, callbacks

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from common.utils import slack_users

# From https://www.geeksforgeeks.org/how-to-import-a-python-module-given-the-full-path/
path = f"{os.path.dirname(os.path.realpath(__file__))}"
steps_path = path + "/steps"

DAG_ID = path.split("/")[-1]

env_args = dag_args.make_env_args(
    dag_id=DAG_ID,
)

input_link_prediction = SourceFileLoader(
    "input_link_prediction", f"{steps_path}/input_link_prediction.py"
).load_module()
output_link_format = SourceFileLoader(
    "output_link_format", f"{steps_path}/output_link_format.py"
).load_module()
link_prediction = SourceFileLoader(
    "link_prediction", f"{steps_path}/link_prediction.py"
).load_module()
sending2postgres = SourceFileLoader(
    "sending2postgres", f"{steps_path}/sending2postgres.py"
).load_module()

dag = DAG(
    env_args["dag_id"],
    default_args=dag_args.make_default_args(
    start_date=pendulum.datetime(2022, 3, 1, tz="Europe/London"),
    retries=0,
    on_failure_callback=callbacks.task_fail_slack_alert(slack_users.CARLOS),
    on_retry_callback=callbacks.task_retry_slack_alert(slack_users.CARLOS),
),
    schedule_interval=env_args["schedule_interval"],
    tags=[env_args["env"], "monthly", "backfill"],
    params=dag_args.load_config(__file__),
    doc_md=dag_args.load_docs(__file__),
)

start = DummyOperator(task_id="start", dag=dag)

# Input link prediction
input_link_prediction_end = input_link_prediction.register(dag, start)

# link prediction
link_predict_end = link_prediction.register(dag, input_link_prediction_end)

# output of link  prediction
output_link_predict_end = output_link_format.register(dag, link_predict_end)

# sending2postgres_end = sending2postgres.register(dag, output_link_predict_end)
