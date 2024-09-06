# airflow imports
# other imports
import os
from datetime import datetime
from importlib.machinery import SourceFileLoader

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from common.utils import callbacks, dag_args

path = f"{os.path.dirname(os.path.realpath(__file__))}"
DAG_ID = path.split("/")[-1]
steps_path = f"{path}/steps"

# VISTIS TABLES MIGRATION
migration_task = SourceFileLoader(
    "visits_migration_tasks", f"{steps_path}/visits_migration_tasks.py"
).load_module()

env_args = dag_args.make_env_args()

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
    params=dag_args.load_config(scope="global"),
    default_args=default_args,
    description="parent dag: scaling_models_creation_trigger, migrates visits and visits scaled tables",
    schedule_interval=env_args["schedule_interval"],
    tags=[env_args["env"], "smc", "externally_triggered"],
    doc_md=dag_args.load_docs(__file__),
) as dag:
    start = EmptyOperator(task_id="start")

    datasets = ["poi_visits_scaled_dataset", "poi_visits_dataset"]

    visits_migration_task = migration_task.register(start=start, dag=dag, datasets=datasets)

    end = EmptyOperator(task_id="end")

    visits_migration_task >> end
