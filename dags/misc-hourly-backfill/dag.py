import os, pendulum
from importlib.machinery import SourceFileLoader

from common.utils import dag_args, callbacks
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.weight_rule import WeightRule

# Relative imports based on the current file location
path = f"{os.path.dirname(os.path.realpath(__file__))}"
steps_path = path + "/steps"

obtain_column_to_list = SourceFileLoader(
    "obtain_column_to_list", f"{steps_path}/obtain_column_to_list.py"
).load_module()

run_validation = SourceFileLoader(
    "run_validation", f"{steps_path}/run_validation.py"
).load_module()

DAG_ID = path.split("/")[-1]

env_args = dag_args.make_env_args(
    dag_id=DAG_ID,
)


with DAG(
    env_args["dag_id"],
    default_args=dag_args.make_default_args(
    start_date=pendulum.datetime(2022, 3, 1, tz="Europe/London"),
    retries=0,
    weight_rule=WeightRule.UPSTREAM,
),
    schedule_interval=env_args["schedule_interval"],
    tags=[env_args["env"], "misc"],
    params=dag_args.load_config(__file__),
    doc_md=dag_args.load_docs(__file__),
) as dag:

    start = EmptyOperator(task_id="start")

    obtain_column_to_list_end = obtain_column_to_list.register(start=start, dag=dag)

    run_validation_end = run_validation.register(start=obtain_column_to_list_end, dag=dag)

    end = EmptyOperator(task_id="end")

    run_validation_end >> end
