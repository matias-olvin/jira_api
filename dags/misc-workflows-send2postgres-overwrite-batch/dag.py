"""
DAG ID: send2postgres
"""
import os
from importlib.machinery import SourceFileLoader

import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from common.utils import dag_args, callbacks

path = f"{os.path.dirname(os.path.realpath(__file__))}"
steps_path = path + "/steps"

DAG_ID = path.split("/")[-1]

env_args = dag_args.make_env_args(
    dag_id=DAG_ID,
)


send_to_database = SourceFileLoader(
    "send_to_database", f"{steps_path}/send_to_database.py"
).load_module()

with DAG(
    f"{env_args['dag_id']}-dev",
    default_args=dag_args.make_default_args(
    start_date=pendulum.datetime(2022, 3, 10, tz="Europe/London"),
    retries=3,
    email="jake@olvin.com",
    email_on_failure=True,
    email_on_retry=False,
    on_failure_callback=callbacks.task_fail_slack_alert("U02KJ556S1H"),
),
    schedule_interval=env_args["schedule_interval"],
    tags=[env_args["env"], "misc"],
    params=dag_args.load_config(__file__),
    doc_md=dag_args.load_docs(__file__),
) as dag:
    start = DummyOperator(task_id="start", dag=dag)
    send_to_database_end = send_to_database.register(dag, start, db="dev")


with DAG(
    f"{env_args['dag_id']}-staging",
    default_args=dag_args.make_default_args(
    start_date=pendulum.datetime(2022, 3, 10, tz="Europe/London"),
    retries=3,
    email="jake@olvin.com",
    email_on_failure=True,
    email_on_retry=False,
    on_failure_callback=callbacks.task_fail_slack_alert("U02KJ556S1H"),
),
    schedule_interval=env_args["schedule_interval"],
    tags=[env_args["env"], "misc"],
    params=dag_args.load_config(__file__),
    doc_md=dag_args.load_docs(__file__),
) as dag:
    start = DummyOperator(task_id="start", dag=dag)
    send_to_database_end = send_to_database.register(dag, start, db="staging")
