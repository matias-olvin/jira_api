import os
from importlib.machinery import SourceFileLoader

import airflow
import pendulum
from airflow.operators.empty import EmptyOperator

# local imports
from common.utils import callbacks, dag_args, slack_users

path = f"{os.path.dirname(os.path.realpath(__file__))}"

DAG_ID = path.split("/")[-1]
ENV_ARGS = dag_args.make_env_args(schedule_interval="@daily")

metrics = SourceFileLoader("metrics", f"{path}/steps/metrics.py").load_module()

default_args = {
    "depends_on_past": True,
}

with airflow.DAG(
    DAG_ID,
    schedule_interval=ENV_ARGS["schedule_interval"],
    start_date=pendulum.datetime(2024, 5, 10, tz="Europe/London"),
    end_date=pendulum.datetime(2024, 6, 12, tz="Europe/London"),
    catchup=True,
    is_paused_upon_creation=True,
    default_args=default_args,
    max_active_runs=1,
    params=dag_args.load_config(__file__),
    on_failure_callback=callbacks.task_fail_slack_alert(slack_users.JAKE),
) as dag:
    start = EmptyOperator(task_id="start")
    metrics_end = metrics.register(start=start)
    end = EmptyOperator(task_id="end")
    metrics_end >> end
