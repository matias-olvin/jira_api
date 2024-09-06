import os
from datetime import datetime
from importlib.machinery import SourceFileLoader

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from common.utils import dag_args, callbacks

# import places modules
path = f"{os.path.dirname(os.path.realpath(__file__))}"
DAG_ID = path.split("/")[-1]

poi_visits = SourceFileLoader(
    "poi_visits", f"{path}/steps/places/poi_visits.py"
).load_module()
poi_stats_tables = SourceFileLoader(
    "poi_stats_tables", f"{path}/steps/places/stats_tables.py"
).load_module()
poi_visits_scaled = SourceFileLoader(
    "poi_visits_scaled", f"{path}/steps/places/poi_visits_scaled.py"
).load_module()
monitor_daily_stats_poi = SourceFileLoader(
    "monitor_daily_stats_poi", f"{path}/steps/places/monitoring/monitor_daily_stats.py"
).load_module()

env_args = dag_args.make_env_args()
default_args = dag_args.make_default_args(
    start_date=datetime(2022, 9, 16),
    retries=0,
    on_failure_callback=callbacks.task_fail_slack_alert("U034YDXAD1R"),
    on_retry_callback=callbacks.task_retry_slack_alert("U034YDXAD1R"),
)

with DAG(
    DAG_ID,
    default_args=default_args,
    schedule_interval=env_args["schedule_interval"],
    tags=[env_args["env"], "smc", "backfill"],
    params=dag_args.load_config(__file__),
    doc_md=dag_args.load_docs(__file__),
) as dag:
    start = DummyOperator(task_id="start", dag=dag)

    # Post-SMC Places Tasks
    poi_visits_end = poi_visits.register(dag=dag, start=start)
    poi_stats_tables_end = poi_stats_tables.register(dag=dag, start=poi_visits_end)
    poi_visits_scaled_end = poi_visits_scaled.register(
        dag=dag, start=poi_stats_tables_end
    )
    monitor_daily_stats_poi_end = monitor_daily_stats_poi.register(
        dag=dag, start=poi_visits_scaled_end
    )
