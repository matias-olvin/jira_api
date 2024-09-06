"""
DAG ID: reviews
"""
import os
from datetime import datetime
from importlib.machinery import SourceFileLoader

# other imports
from common.utils import dag_args, callbacks
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

path = f"{os.path.dirname(os.path.realpath(__file__))}"
DAG_ID = path.split("/")[-1]

steps_path = f"{path}/steps/"
google_places_api = SourceFileLoader(
    "google_places_api", f"{steps_path}/google_places_api.py"
).load_module()

env_args = dag_args.make_env_args(
    dag_id=DAG_ID,
)

with DAG(
    env_args["dag_id"],
    default_args=dag_args.make_default_args(
        start_date=datetime(2023, 7, 8),
        on_failure_callback=callbacks.task_fail_slack_alert(
            "U02KJ556S1H", channel=env_args["env"]
        ),
    ),
    schedule_interval=env_args["schedule_interval"],
    params=dag_args.load_config(__file__),
    doc_md=dag_args.load_docs(__file__),
) as dag:
    start = DummyOperator(task_id="start", dag=dag)
    google_places_api_end = google_places_api.register(dag, start)
