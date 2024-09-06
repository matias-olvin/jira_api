"""
DAG ID: predicthq_events_collection
"""

import os
from datetime import datetime
from importlib.machinery import SourceFileLoader

from common.utils import dag_args, callbacks
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

path = f"{os.path.dirname(os.path.realpath(__file__))}"
DAG_ID = path.split("/")[-1]

steps_path = f"{path}/steps/"
predicthq_api_call = SourceFileLoader(
    "predicthq_api_call", f"{steps_path}/predicthq_api_call.py"
).load_module()
preprocessing_update_data = SourceFileLoader(
    "preprocessing_update_data", f"{steps_path}/preprocessing_update_data.py"
).load_module()
full_event_data_upsert = SourceFileLoader(
    "full_event_data_upsert", f"{steps_path}/full_event_data_upsert.py"
).load_module()
full_regressors_collection = SourceFileLoader(
    "full_regressors_collection", f"{steps_path}/full_regressors_collection.py"
).load_module()
updating_event_metrics_table = SourceFileLoader(
    "updating_event_metrics_table", f"{steps_path}/updating_event_metrics_table.py"
).load_module()
test_full_event_data = SourceFileLoader(
    "test_full_event_data", f"{steps_path}/tests/test_full_event_data.py"
).load_module()
test_event_identifiers = SourceFileLoader(
    "test_event_identifiers", f"{steps_path}/tests/test_event_identifiers.py"
).load_module()

env_args = dag_args.make_env_args(
    dag_id=DAG_ID,
    schedule_interval="0 6 * * *",
)

with DAG(
    env_args["dag_id"],
    default_args=dag_args.make_default_args(
        start_date=datetime(2022, 9, 14),
        on_failure_callback=callbacks.task_fail_slack_alert(
            "U034YDXAD1R", channel=env_args["env"]
        ),  # Jake
    ),
    schedule_interval=env_args["schedule_interval"],
    params=dag_args.load_config(__file__),
    doc_md=dag_args.load_docs(__file__),
) as dag:
    # start
    start = DummyOperator(task_id="start", dag=dag)

    predicthq_api_call_end = predicthq_api_call.register(dag, start)
    preprocessing_update_data_end = preprocessing_update_data.register(
        dag, predicthq_api_call_end
    )
    full_event_data_upsert_end = full_event_data_upsert.register(
        dag, preprocessing_update_data_end
    )
    full_regressors_collection_end = full_regressors_collection.register(
        dag, full_event_data_upsert_end
    )
    updating_event_metrics_table_end = updating_event_metrics_table.register(
        dag, full_regressors_collection_end
    )

    # verification
    test_full_event_data_end = test_full_event_data.register(
        dag, full_event_data_upsert_end
    )
    test_event_identifiers_end = test_event_identifiers.register(
        dag, full_regressors_collection_end
    )
