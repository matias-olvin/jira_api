import os
from datetime import datetime, timedelta
from importlib.machinery import SourceFileLoader

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.weight_rule import WeightRule
from common.utils import callbacks, dag_args

path = f"{os.path.dirname(os.path.realpath(__file__))}"
DAG_ID = path.split("/")[-1]

steps_path = f"{path}/steps/"
weather_update = SourceFileLoader(
    "weather_update", f"{steps_path}/weather_update.py"
).load_module()
data_load_bq = SourceFileLoader(
    "data_load_bq", f"{steps_path}/data_load_bq.py"
).load_module()
data_load_database = SourceFileLoader(
    "data_load_database", f"{steps_path}/data_load_database.py"
).load_module()
# data_delete = SourceFileLoader("data_delete", f"{steps_path}/data_delete.py").load_module()

env_args = dag_args.make_env_args(
    schedule_interval="@daily",
)

default_args = dag_args.make_default_args(
    start_date=datetime(2022, 9, 14),
    on_failure_callback=callbacks.task_fail_slack_alert(
        "U02KJ556S1H", channel=env_args["env"]
    ),
    on_retry_callback=callbacks.task_retry_slack_alert(
        "U02KJ556S1H", channel=env_args["env"]
    ),
    retries=3,
    weight_rule=WeightRule.UPSTREAM,
)

# Historical dag
with DAG(
    DAG_ID,
    default_args=default_args,
    schedule_interval=env_args["schedule_interval"],
    dagrun_timeout=timedelta(hours=12),
    params=dag_args.load_config(scope="global"),
    doc_md=dag_args.load_docs(__file__),
) as dag_historical:

    start = EmptyOperator(task_id="start")

    weather_update_end = weather_update.register(
        start=start, dag=dag_historical, mode="append", output_folder="historical"
    )
    data_load_bq_end = data_load_bq.register(
        start=weather_update_end, dag=dag_historical, output_folder="historical"
    )

    end = EmptyOperator(task_id="end")

    data_load_bq_end >> end
