"""
DAG ID: demographics_pipeline
"""
import os, pendulum
from datetime import datetime
from importlib.machinery import SourceFileLoader
from common.utils import dag_args, callbacks

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.weight_rule import WeightRule
from common.utils import slack_users

# Relative imports based on the current file location
path = f"{os.path.dirname(os.path.realpath(__file__))}"
steps_path = path + "/steps"

DAG_ID = path.split("/")[-1]

env_args = dag_args.make_env_args(
    dag_id=DAG_ID,
)


home_finder = SourceFileLoader(
    "home_finder", f"{steps_path}/home_finder.py"
).load_module()
zip_demographics = SourceFileLoader(
    "zip_demographics", f"{steps_path}/zip_demographics.py"
).load_module()

dag = DAG(
    env_args["dag_id"],
    default_args=dag_args.make_default_args(
    start_date=pendulum.datetime(2022, 8, 16, tz="Europe/London"),
    retries=0,
    email="pablo@olvin.com",
    email_on_failure=True,
    email_on_retry=False,
    on_failure_callback=callbacks.task_fail_slack_alert(slack_users.CARLOS, slack_users.MATIAS),
    weight_rule=WeightRule.UPSTREAM,
),
    schedule_interval=env_args["schedule_interval"],
    tags=[env_args["env"], "monthly"],
    params=dag_args.load_config(__file__),
    doc_md=dag_args.load_docs(__file__),
)

start = DummyOperator(task_id="start", dag=dag)

home_finder_end = home_finder.register(dag, start)
zip_demographics_end = zip_demographics.register(start=home_finder_end, dag=dag)

end = DummyOperator(task_id="end", dag=dag)
zip_demographics_end >> end
