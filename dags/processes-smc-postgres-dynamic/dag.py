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

# PLACES
sgplaceraw_tasks = SourceFileLoader(
    "sgplaceraw-tasks", f"{path}/steps/sgplaceraw/sgplaceraw-tasks.py"
).load_module()
sgplaceraw_tests = SourceFileLoader(
    "sgplaceraw-tests", f"{path}/steps/sgplaceraw/sgplaceraw-tests.py"
).load_module()
# BRANDS
sgbrandraw_tasks = SourceFileLoader(
    "sgbrandraw-tasks", f"{path}/steps/sgbrandraw/sgbrandraw-tasks.py"
).load_module()
sgbrandraw_tests = SourceFileLoader(
    "sgbrandraw-tests", f"{path}/steps/sgbrandraw/sgbrandraw-tests.py"
).load_module()


env_args = dag_args.make_env_args()

default_args = dag_args.make_default_args(
    start_date=pendulum.datetime(2022, 3, 1, tz="Europe/London"),
    retries=2,
    on_failure_callback=callbacks.task_fail_slack_alert(
        "U05M60N8DMX", # Matias
        channel=env_args["env"]
    ),
    on_retry_callback=callbacks.task_retry_slack_alert(
        "U05M60N8DMX", # Matias
        channel=env_args["env"]
    )
)

with DAG(
    DAG_ID,
    default_args=default_args,
    description="Update `postgres` tables for SMC.",
    schedule_interval=env_args["schedule_interval"],
    tags=[env_args["env"], "smc", "externally_triggered"],
    doc_md=dag_args.load_docs(__file__),
    params=dag_args.load_config(scope="global"),
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    sgplaceraw_tasks_end = sgplaceraw_tasks.register(start)
    sgplaceraw_tests_end = sgplaceraw_tests.register(sgplaceraw_tasks_end)

    sgbrandraw_tasks_end = sgbrandraw_tasks.register(start)
    sgbrandraw_tests_end = sgbrandraw_tests.register(sgbrandraw_tasks_end)

    [sgplaceraw_tests_end, sgbrandraw_tests_end] >> end