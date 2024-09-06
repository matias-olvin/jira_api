# airflow imports
# other imports
import os
from importlib.machinery import SourceFileLoader

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from common.utils import callbacks, dag_args

path = f"{os.path.dirname(os.path.realpath(__file__))}"
DAG_ID = path.split("dags/")[-1].replace("/", "-")
FOLDER = path.split("/")[-1]

# REGRESSORS TRIGGER SENSOR GROUP
trigger_sensor_group = SourceFileLoader(
    "trigger_sensor_group", f"{path}/steps/trigger_sensor_group.py"
).load_module()

env_args = dag_args.make_env_args(
    schedule_interval=None
)

default_args = dag_args.make_default_args(
    start_date=pendulum.datetime(2022, 3, 1, tz="Europe/London"),
    retries=0,
    on_retry_callback=callbacks.task_retry_slack_alert(
        "U05M60N8DMX", # Matias
        channel=env_args["env"]
    ),
    on_failure_callback=callbacks.task_fail_slack_alert(
        "U05M60N8DMX", # Matias
        channel=env_args["env"]
    )
)

with DAG(
    DAG_ID,
    default_args=default_args,
    description="Update `regressors` tables for SMC",
    schedule_interval=env_args["schedule_interval"],
    tags=[env_args["env"], "smc", "externally_triggered"],
    doc_md=dag_args.load_docs(__file__),
    params=dag_args.load_config(scope="global"),
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    regressors = [
        "dag-sources-regressors-covid-dictionary",
        "dag-sources-regressors-predicthq-dictionary",
        "dag-sources-regressors-weather-dictionary",
        "dag-sources-regressors-smc-time-factors-holidays-dictionary",
        "dag-sources-regressors-smc-time-factors-group-residuals-dictionary",
    ]

    regressors_tasks_end = [
        trigger_sensor_group.register(start=start, dag=dag, trigger_dag_id=regressor, poke_interval=60 * 2) for regressor in regressors
    ]

    regressors_tasks_end >> end