import os
from datetime import datetime
from importlib.machinery import SourceFileLoader

from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from common.utils import callbacks, dag_args

path = f"{os.path.dirname(os.path.realpath(__file__))}"
steps_path = f"{path}/steps"
DAG_ID = path.split("/")[-1]

query_events_dictionary = SourceFileLoader(
    "query_events_dictionary", f"{steps_path}/query_events_dictionary.py"
).load_module()

env_args = dag_args.make_env_args(
    schedule_interval=None,
)

default_args = dag_args.make_default_args(
    start_date=datetime(2022, 9, 14),
    on_failure_callback=callbacks.task_fail_slack_alert(
        "U05M60N8DMX", channel=env_args["env"]  # Matias
    ),
)

with DAG(
    DAG_ID,
    default_args=default_args,
    schedule_interval=env_args["schedule_interval"],
    params=dag_args.load_config(scope="global"),
    doc_md=dag_args.load_docs(__file__),
) as dag:

    start = EmptyOperator(task_id="start")

    query_events_dictionary_end = query_events_dictionary.register(start=start, dag=dag)

    end = EmptyOperator(task_id="end")

    query_events_dictionary_end >> end
