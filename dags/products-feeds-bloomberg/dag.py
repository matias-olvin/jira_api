"""
DAG ID: bloomberg_feed
"""
import os
from datetime import datetime
from importlib.machinery import SourceFileLoader

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.weight_rule import WeightRule

from common.utils import dag_args, callbacks

path = f"{os.path.dirname(os.path.realpath(__file__))}"
DAG_ID = path.split("/")[-1]

poi_visits_to_places = SourceFileLoader(
    "poi_visits_to_places", f"{path}/steps/poi_visits_to_places.py"
).load_module()
export_to_gcs = SourceFileLoader(
    "export_to_gcs", f"{path}/steps/export_to_gcs.py"
).load_module()

env_args = dag_args.make_env_args()
default_args = dag_args.make_default_args(
    start_date=datetime(2022, 9, 16),
    depends_on_past=True,
    wait_for_downstream=True,
    retries=3,
    on_failure_callback=callbacks.task_fail_slack_alert("U0180QS8MFV"),
    weight_rule=WeightRule.UPSTREAM,
)

dag = DAG(
    DAG_ID,
    default_args=default_args,
    schedule_interval=env_args["schedule_interval"],  # "0 3 * * *",
    tags=[env_args["env"], "daily"],
    params=dag_args.load_config(__file__),
    doc_md=dag_args.load_docs(__file__),
)

start = DummyOperator(task_id="start", dag=dag)
poi_visits_end = poi_visits_to_places.register(dag, start)
export_to_gcs_end = export_to_gcs.register(
    dag, poi_visits_end
)
