import os
from importlib.machinery import SourceFileLoader

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from common.utils import dag_args

path = f"{os.path.dirname(os.path.realpath(__file__))}"
steps_path = path + "/steps"

DAG_ID = path.split("/")[-1]

env_args = dag_args.make_env_args()

get_execution_date = SourceFileLoader(
    "get_execution_date", f"{steps_path}/get_execution_date.py"
).load_module()
monthly_update_tasks = SourceFileLoader(
    "monthly_update_tasks", f"{steps_path}/monthly_update_tasks.py"
).load_module()
postgres_tasks = SourceFileLoader(
    "postgres_tasks", f"{steps_path}/postgres_tasks.py"
).load_module()
set_update_complete = SourceFileLoader(
    "set_update_complete", f"{steps_path}/set_update_complete.py"
).load_module()

default_args = dag_args.make_default_args(
    start_date=pendulum.datetime(2022, 8, 16, tz="Europe/London"),
    retries=0,
    provide_context=True,
)

with DAG(
    DAG_ID,
    default_args=default_args,
    schedule_interval=env_args["schedule_interval"],
    tags=[env_args["env"], "monthly"],
    params=dag_args.load_config(scope="global"),
    doc_md=dag_args.load_docs(__file__),
) as dag:
    start = EmptyOperator(task_id="start", dag=dag)

    get_execution_date_end = get_execution_date.register(start=start, dag=dag)

    monthly_update_tasks_end = monthly_update_tasks.register(
        start=get_execution_date_end, dag=dag
    )
    postgres_tasks_end = postgres_tasks.register(
        start=monthly_update_tasks_end, dag=dag
    )
    set_update_complete_end = set_update_complete.register(
        start=postgres_tasks_end, dag=dag
    )
