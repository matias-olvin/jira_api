"""
DAG ID: visits_estimation_model_development_hourly
"""
import os
from datetime import datetime
from importlib.machinery import SourceFileLoader

from common.utils import dag_args, callbacks
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator


path = f"{os.path.dirname(os.path.realpath(__file__))}"
DAG_ID = path.split("/")[-1]

triggering_sns = SourceFileLoader(
    "triggering_sns", f"{path}/steps/triggering_sns.py"
).load_module()
input_preparation = SourceFileLoader(
    "input_preparation", f"{path}/steps/input_preparation.py"
).load_module()
run_model = SourceFileLoader("model", f"{path}/steps/model.py").load_module()
# triggering_validation = SourceFileLoader("triggering_validation", f"{path}/steps/triggering_validation.py").load_module()

env_args = dag_args.make_env_args(
    dag_id=DAG_ID,
)

with DAG(
    env_args["dag_id"],
    default_args=dag_args.make_default_args(
        start_date=datetime(2022, 1, 8),
        retries=0,
        on_failure_callback=callbacks.task_fail_slack_alert("UU2ER1M3R"),
    ),
    schedule_interval=env_args["schedule_interval"],
    params=dag_args.load_config(__file__),
    doc_md=dag_args.load_docs(__file__),
) as dag:
    start = DummyOperator(task_id="start", dag=dag)

    # Trigger sns
    triggering_sns_end = triggering_sns.register(dag, start)

    # Prepare input
    input_preparation_end = input_preparation.register(dag, triggering_sns_end)

    # Run model
    run_model_end = run_model.register(dag, input_preparation_end)

    # Trigger validation
    # triggering_validation_end = triggering_validation.register(dag, triggering_model_end)
