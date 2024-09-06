"""
DAG ID: dynamic_places
"""
import os
from datetime import datetime
from importlib.machinery import SourceFileLoader

from common.utils import dag_args, callbacks, slack_users
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

path = f"{os.path.dirname(os.path.realpath(__file__))}"
DAG_ID = path.split("/")[-1]
steps_path = f"{path}/steps"

drop_staging_tables = SourceFileLoader(
    "drop_staging_tables", f"{steps_path}/drop_staging_tables.py"
).load_module()

load_sg_brands = SourceFileLoader(
    "load_sg_brands", f"{steps_path}/brands/load_sg_brands.py"
).load_module()
upsert_brands_history = SourceFileLoader(
    "upsert_brands_history", f"{steps_path}/brands/upsert_brands_history.py"
).load_module()

monitor_sg_brands = SourceFileLoader(
    "monitor_sg_brands", f"{steps_path}/monitoring/monitor_sg_brands.py"
).load_module()
monitor_sg_places = SourceFileLoader(
    "monitor_sg_places", f"{steps_path}/monitoring/monitor_sg_places.py"
).load_module()
monitor_sg_raw = SourceFileLoader(
    "monitor_sg_raw", f"{steps_path}/monitoring/monitor_sg_raw.py"
).load_module()

create_sg_places = SourceFileLoader(
    "create_sg_places", f"{steps_path}/places/create_sg_places.py"
).load_module()
lineage_api = SourceFileLoader(
    "lineage_api", f"{steps_path}/places/lineage_api.py"
).load_module()
load_sg_raw_data = SourceFileLoader(
    "load_sg_raw_data", f"{steps_path}/places/load_sg_raw_data.py"
).load_module()
update_places_history = SourceFileLoader(
    "update_places_history", f"{steps_path}/places/update_places_history.py"
).load_module()
upsert_places_history = SourceFileLoader(
    "upsert_places_history", f"{steps_path}/places/upsert_places_history.py"
).load_module()

test_lineage = SourceFileLoader(
    "test_lineage", f"{steps_path}/tests/test_lineage.py"
).load_module()
test_places_history_1 = SourceFileLoader(
    "test_places_history_1", f"{steps_path}/tests/test_places_history_1.py"
).load_module()
test_places_history_2 = SourceFileLoader(
    "test_places_history_2", f"{steps_path}/tests/test_places_history_2.py"
).load_module()
test_sg_brands = SourceFileLoader(
    "test_sg_brands", f"{steps_path}/tests/test_sg_brands.py"
).load_module()
test_sg_places = SourceFileLoader(
    "test_sg_places", f"{steps_path}/tests/test_sg_places.py"
).load_module()
test_sg_raw = SourceFileLoader(
    "test_sg_raw", f"{steps_path}/tests/test_sg_raw.py"
).load_module()
test_ideal_logic = SourceFileLoader(
    "test_ideal_logic", f"{steps_path}/tests/test_ideal_logic.py"
).load_module()


env_args = dag_args.make_env_args(
    dag_id=DAG_ID, 
    schedule_interval="0 0 8 * *"
)

with DAG(
    env_args["dag_id"],
    default_args=dag_args.make_default_args(
        retries=3,
    ),
    description="DAG for dynamic_places pipeline",
    start_date=datetime(2022, 9, 5),
    schedule_interval=env_args["schedule_interval"],  # monthly on 8th day
    on_failure_callback=callbacks.task_fail_slack_alert(slack_users.MATIAS),
    on_success_callback=callbacks.pipeline_end_slack_alert(slack_users.MATIAS),
    params=dag_args.load_config(__file__),
    doc_md=dag_args.load_docs(__file__),
) as places_dag:
    # start dummy
    start = DummyOperator(
        task_id="start",
        dag=places_dag,
    )

    # load raw data
    load_sg_raw_end = load_sg_raw_data.register(
        dag=places_dag,
        start=start,
    )
    load_sg_brands_end = load_sg_brands.register(
        dag=places_dag,
        start=start,
    )
    test_sg_raw_end = test_sg_raw.register(dag=places_dag, start=load_sg_raw_end)
    test_sg_brands_end = test_sg_brands.register(
        dag=places_dag, start=load_sg_brands_end
    )

    # dynamic places tables tasks
    create_sg_places_end = create_sg_places.register(
        dag=places_dag, start=[test_sg_brands_end, test_sg_raw_end]
    )
    test_sg_places_end = test_sg_places.register(
        dag=places_dag,
        start=create_sg_places_end,
    )
    upsert_places_history_end = upsert_places_history.register(
        dag=places_dag,
        start=test_sg_places_end,
    )
    test_places_history_1_end = test_places_history_1.register(
        dag=places_dag,
        start=upsert_places_history_end,
    )
    # lineage API tasks
    lineage_api_end = lineage_api.register(
        dag=places_dag,
        start=test_places_history_1_end,
    )
    test_lineage_end = test_lineage.register(
        dag=places_dag,
        start=lineage_api_end,
    )
    # use lineage data to update places_history
    update_places_history_end = update_places_history.register(
        dag=places_dag,
        start=test_lineage_end,
    )
    test_places_history_2_end = test_places_history_2.register(
        dag=places_dag,
        start=update_places_history_end,
    )
    # testing ideal logic for creating places_dynamic table ---> use when Lineage API functions properly!
    test_ideal_logic_end = test_ideal_logic.register(
        dag=places_dag, start=test_places_history_2_end
    )
    # dynamic brands tables tasks
    upsert_brands_history_end = upsert_brands_history.register(
        dag=places_dag, start=test_sg_places_end
    )

    # monitor data
    monitor_sg_raw_end = monitor_sg_raw.register(
        dag=places_dag,
        start=test_sg_raw_end,
    )
    monitor_sg_brands_end = monitor_sg_brands.register(
        dag=places_dag,
        start=test_sg_brands_end,
    )
    monitor_sg_places_end = monitor_sg_places.register(
        dag=places_dag, start=test_places_history_2_end
    )

    # delete staging data
    drop_staging_tables_end = drop_staging_tables.register(
        dag=places_dag,
        start=[monitor_sg_places_end, monitor_sg_brands_end, upsert_brands_history_end],
    )
