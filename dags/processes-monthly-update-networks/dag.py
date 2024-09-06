"""
DAG ID: networks_pipeline
"""
import os,  pendulum
from datetime import datetime
from importlib.machinery import SourceFileLoader
from common.utils import dag_args, callbacks

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor
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

dag = DAG(
    env_args["dag_id"],
    default_args=dag_args.make_default_args(
    start_date=pendulum.datetime(2022, 3, 1, tz="Europe/London"),
    retries=0,
    on_failure_callback=callbacks.task_fail_slack_alert(slack_users.CARLOS),
    on_retry_callback=callbacks.task_retry_slack_alert(slack_users.CARLOS),
),
    concurrency=2,
    schedule_interval=env_args["schedule_interval"],
    tags=[env_args["env"], "monthly"],
    params=dag_args.load_config(__file__),
    doc_md=dag_args.load_docs(__file__),
)

start = DummyOperator(task_id="start", dag=dag)

wait_for_scaled_poi_visits = ExternalTaskSensor(
    task_id="wait_for_scaled_poi_visits",
    external_dag_id="sources-raw-visits-daily",
    external_task_id="query_naics_filtering",
    dag=dag,
    poke_interval=60 * 20,
    execution_date_fn=lambda dt: dt.replace(
        month=dt.month + 1, day=dt.day, hour=3, minute=0, second=0, microsecond=0
    )
    .add(days=int(Variable.get("latency_days_visits")))
    .subtract(days=dag.params["early_computation_days"]),
    mode="reschedule",
    timeout=11 * 24 * 60 * 60,
)
start >> wait_for_scaled_poi_visits

# Input link prediction
input_link_prediction_end = input_link_prediction.register(
    dag, wait_for_scaled_poi_visits
)

# link prediction
link_predict_end = link_prediction.register(dag, input_link_prediction_end)

# output of link  prediction
output_link_predict_end = output_link_format.register(dag, link_predict_end)
