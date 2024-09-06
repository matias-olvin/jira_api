"""
DAG ID: sg_agg_stats
"""
import os,pendulum
from datetime import datetime
from importlib.machinery import SourceFileLoader
from common.utils import dag_args, callbacks
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.weight_rule import WeightRule
from common.utils import slack_users

# Relative imports based on the current file location
path = f"{os.path.dirname(os.path.realpath(__file__))}"
steps_path = path + "/steps/"

DAG_ID = path.split("/")[-1]

env_args = dag_args.make_env_args(
    dag_id=DAG_ID,
)

process_data = SourceFileLoader(
    "process_data", steps_path + "process_data.py"
).load_module()

# Create monthly DAG
with DAG(
    env_args["dag_id"],
    default_args=dag_args.make_default_args(
    start_date=pendulum.datetime(2022, 7, 1, tz="Europe/London"),
    retries=0,
    on_failure_callback=callbacks.task_fail_slack_alert(slack_users.CARLOS),
    on_retry_callback=callbacks.task_retry_slack_alert(slack_users.CARLOS),
    weight_rule=WeightRule.UPSTREAM,
),
    concurrency=4,
    schedule_interval=env_args["schedule_interval"],
    tags=[env_args["env"], "monthly"],
    # params=dag_args.load_config(__file__, path="../config.yaml"),
    params=dag_args.load_config(__file__),
    # doc_md=dag_args.load_docs(__file__),
    user_defined_macros={},
) as dag:
    mode = "update"

    start = DummyOperator(task_id="start", dag=dag)
    process_data_end = process_data.register(dag, start)
