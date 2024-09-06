"""
DAG ID: demographics_pipeline
"""
import os, pendulum
from importlib.machinery import SourceFileLoader

from common.utils import dag_args, callbacks
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.weight_rule import WeightRule

# Relative imports based on the current file location
path = f"{os.path.dirname(os.path.realpath(__file__))}"
steps_path = path + "/steps"
train_areas = SourceFileLoader(
    "train_areas", f"{steps_path}/train_areas.py"
).load_module()

trade_areas_input = SourceFileLoader(
    "trade_areas_input", f"{steps_path}/generate_input.py"
).load_module()

trade_areas_output = SourceFileLoader(
    "trade_areas_output", f"{steps_path}/generate_output.py"
).load_module()

trade_areas_tests = SourceFileLoader(
    "trade_areas_tests", f"{steps_path}/run_tests.py"
).load_module()

send_to_postgres = SourceFileLoader(
    "send_to_postgres", f"{steps_path}/send_to_postgres.py"
).load_module()

DAG_ID = path.split("/")[-1]

env_args = dag_args.make_env_args(
    dag_id=DAG_ID,
)


with DAG(
    env_args["dag_id"],
    default_args=dag_args.make_default_args(
    start_date=pendulum.datetime(2022, 8, 16, tz="Europe/London"),
    retries=0,
    on_failure_callback=callbacks.task_fail_slack_alert(
        "U05FN3F961X",  # Carlos
    ),
    weight_rule=WeightRule.UPSTREAM,
),
    schedule_interval=env_args["schedule_interval"],
    tags=[env_args["env"], "misc"],
    params=dag_args.load_config(scope="global"),
    doc_md=dag_args.load_docs(__file__),
) as dag:

    start = EmptyOperator(task_id="start")

    trade_areas_input_end = trade_areas_input.register(start=start, dag=dag)

    train_areas_end = train_areas.register(start=trade_areas_input_end, dag=dag)

    trade_areas_output_end = trade_areas_output.register(start=train_areas_end, dag=dag)

    trade_areas_tests_end = trade_areas_tests.register(start=trade_areas_output_end, dag=dag)

    send_to_postgres_end = send_to_postgres.register(start=trade_areas_tests_end, dag=dag, trigger_dag_id="dag-misc-workflows-send2postgres-overwrite-dev")

    end = EmptyOperator(task_id="end")
    send_to_postgres_end >> end
