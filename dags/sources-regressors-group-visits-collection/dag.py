import os
from datetime import datetime, timedelta
from importlib.machinery import SourceFileLoader

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from common.utils import dag_args, callbacks, slack_users

path = f"{os.path.dirname(os.path.realpath(__file__))}"
DAG_ID = path.split("/")[-1]
steps_path = f"{path}/steps"

group_visits_coll = SourceFileLoader(
    "group_visits_coll", f"{steps_path}/group_visits_coll.py"
).load_module()

env_args = dag_args.make_env_args(
    schedule_interval="0 0 10 * *",
)

default_args = dag_args.make_default_args(
    start_date=datetime(2022, 9, 14),
    retries=0,
)

with DAG(
    DAG_ID,
    default_args=default_args,
    concurrency=12,
    schedule_interval=env_args["schedule_interval"],  # 10th day of month at 00:00
    tags=[env_args["env"], "monthly"],
    dagrun_timeout=timedelta(hours=12),
    params=dag_args.load_config(scope="global"),
    doc_md=dag_args.load_docs(__file__),
    on_failure_callback=callbacks.task_fail_slack_alert(slack_users.MATIAS, channel=env_args["env"]),
) as dag:

    start = EmptyOperator(task_id="start")

    group_visits_coll_end = group_visits_coll.register(start=start, dag=dag)
