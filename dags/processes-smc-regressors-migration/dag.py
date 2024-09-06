import os
from importlib.machinery import SourceFileLoader

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from common.utils import callbacks, dag_args

path = f"{os.path.dirname(os.path.realpath(__file__))}"
DAG_ID = path.split("/")[-1]
steps_path = f"{path}/steps"

# SCALING MODELS CREATION
regressors_migration_script = SourceFileLoader(
    "regressors_migration_tasks", f"{steps_path}/migrate_regressor_tables.py"
).load_module()

events_migration_script = SourceFileLoader(
    "events_migration_tasks", f"{steps_path}/migrate_event_tables.py"
).load_module()

env_args = dag_args.make_env_args()

default_args = dag_args.make_default_args(
    start_date=pendulum.datetime(2022, 3, 1, tz="Europe/London"),
    retries=0,
    on_retry_callback=callbacks.task_retry_slack_alert(
        "U05M60N8DMX", channel=env_args["env"]
    ),
    on_failure_callback=callbacks.task_fail_slack_alert(
        "U05M60N8DMX", channel=env_args["env"]
    ),
)

regressor_tables = [
    "covid_dictionary_table",
    "covid_dictionary_zipcode_table",
    "holidays_dictionary_table",
    "holidays_dictionary_zipcode_table",
    "weather_dictionary_table",
    "group_visits_dictionary_table",
    "group_visits_dictionary_zipcode_table",
    "predicthq_events_dictionary_table",
]

event_tables = ["holidays_dictionary_table", "holidays_dictionary_zipcode_table"]

with DAG(
    DAG_ID,
    params=dag_args.load_config(scope="global"),
    default_args=default_args,
    description="parent dag: scaling_models_creation_trigger, child dag is run to migrate regressor tables",
    schedule_interval=env_args["schedule_interval"],
    tags=[env_args["env"], "smc", "externally_triggered"],
    doc_md=dag_args.load_docs(__file__),
) as dag:
    start = EmptyOperator(task_id="start")

    regressors_migration_task = regressors_migration_script.register(
        start=start, dag=dag, regressor_tables=regressor_tables
    )

    events_migration_task = events_migration_script.register(
        start=start, dag=dag, event_tables=event_tables
    )

    end = EmptyOperator(task_id="end")

    [regressors_migration_task, events_migration_task] >> end
