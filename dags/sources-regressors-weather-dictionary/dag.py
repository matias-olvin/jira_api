import os
from datetime import datetime
from importlib.machinery import SourceFileLoader

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from common.utils import dag_args
from common.utils.callbacks import task_fail_slack_alert, task_retry_slack_alert

path = f"{os.path.dirname(os.path.realpath(__file__))}"
DAG_ID = path.split("/")[-1]
steps_path = f"{path}/steps"

weather_dictionary = SourceFileLoader(
    "weather_dictionary", f"{steps_path}/weather_dictionary.py"
).load_module()

env_args = dag_args.make_env_args(
    schedule_interval=None,
)

default_args = dag_args.make_default_args(
    start_date=datetime(2022, 9, 14),
    on_failure_callback=task_fail_slack_alert("U02KJ556S1H", channel=env_args["env"]),
    on_retry_callback=task_retry_slack_alert("U02KJ556S1H", channel=env_args["env"]),
)

with DAG(
    DAG_ID,
    default_args=default_args,
    schedule_interval=env_args["schedule_interval"],
    params=dag_args.load_config(scope="global"),
    doc_md=dag_args.load_docs(__file__),
) as dag:
    start = EmptyOperator(task_id="start")

    weather_dictionary_end = weather_dictionary.register(
        start=start, dag=dag, env=env_args["env"]
    )

    end = EmptyOperator(task_id="end")

    weather_dictionary_end >> end
