import os
from importlib.machinery import SourceFileLoader

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from common.utils import callbacks, dag_args

path = f"{os.path.dirname(os.path.realpath(__file__))}"
DAG_ID = path.split("/")[-1]
steps_path = f"{path}/steps"

migrate_places_tables_task = SourceFileLoader(
    "migrate_places_tables", f"{steps_path}/migrate_places_tables_dynamic_task.py"
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

# format: table_to_be_migrated.destination_dataset
# values should be keys from global config
tables_list = [
    "sgbrandraw_table.postgres_dataset",
    "sgplaceraw_table.postgres_dataset",
    "places_dynamic_table.sg_places_dataset",
    "brands_dynamic_table.sg_places_dataset",
    "olvin_brand_ids_table.sg_places_dataset",
    "spend_patterns_static_table.sg_places_dataset",
    "places_week_array_dynamic_table.sg_places_dataset",
    "malls_base_table.sg_places_dataset",
    "sg_places_filter_table.sg_places_dataset",
    "places_manual_table.sg_places_dataset",
]

with DAG(
    DAG_ID,
    params=dag_args.load_config(scope="global"),
    default_args=default_args,
    description="Parent DAG: scaling_models_creation_trigger, migrate places tables",
    schedule_interval=env_args["schedule_interval"],
    tags=[env_args["env"], "smc", "externally_triggered"],
    doc_md=dag_args.load_docs(__file__),
) as dag:
    start = EmptyOperator(task_id="start")

    places_migration_end = migrate_places_tables_task.register(
        start=start, dag=dag, tables=tables_list
    )
