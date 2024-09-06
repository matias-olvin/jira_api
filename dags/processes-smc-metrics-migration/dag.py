# airflow imports
# other imports
import os
from importlib.machinery import SourceFileLoader

from airflow import DAG
import pendulum
from airflow.operators.empty import EmptyOperator

from common.utils import dag_args, callbacks

path = f"{os.path.dirname(os.path.realpath(__file__))}"
DAG_ID = path.split("/")[-1]
steps_path = f"{path}/steps"

migrate_places_tables_task = SourceFileLoader(
    'migrate_places_tables', f"{steps_path}/metrics_migration_tasks.py"
).load_module()

env_args = dag_args.make_env_args()

default_args = dag_args.make_default_args(
    start_date=pendulum.datetime(2022, 3, 1, tz="Europe/London"),
    retries=0,
    on_retry_callback=callbacks.task_retry_slack_alert(
        "U034YDXAD1R",
        channel=env_args["env"]
    ),
    on_failure_callback=callbacks.task_fail_slack_alert(
        "U034YDXAD1R",
        channel=env_args["env"]
    )
)

with DAG(
    DAG_ID,
    params=dag_args.load_config(scope="global"),
    default_args=default_args,
    description="Parent DAG: scaling_models_creation_trigger, migrate metrics tables",
    schedule_interval=env_args["schedule_interval"],
    tags=[env_args["env"], "smc", "externally_triggered"],
    doc_md=dag_args.load_docs(__file__),
) as dag:

    tables_list = [
        "day_stats_visits_table",
        "day_stats_visits_scaled_table",
        "time_series_analysis_visits_block_groundtruth_table"
    ]

    start = EmptyOperator(task_id="start")

    places_migration_end = migrate_places_tables_task.register(start=start, dag=dag, tables=tables_list)