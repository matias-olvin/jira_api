import os
from importlib.machinery import SourceFileLoader

import airflow
import pendulum
from airflow.operators.empty import EmptyOperator
from common.utils import callbacks, dag_args

path = f"{os.path.dirname(os.path.realpath(__file__))}"
DAG_ID = path.split("/")[-1]

data_export = SourceFileLoader(
    "data_export", f"{path}/steps/data_export.py"
).load_module()
instance_tasks = SourceFileLoader(
    "instance_tasks", f"{path}/steps/instance_tasks.py"
).load_module()
data_load = SourceFileLoader("data_load", f"{path}/steps/data_load.py").load_module()

env_args = dag_args.make_env_args()
default_args = dag_args.make_default_args(
    start_date=pendulum.datetime(2023, 10, 1, tz="Europe/London"),
    retries=2,
    on_failure_callback=callbacks.task_fail_slack_alert("U034YDXAD1R", env_args["env"]),
)

with airflow.DAG(
    DAG_ID,
    default_args=default_args,
    schedule_interval=env_args["schedule_interval"],  # None
    description="DAG for pulling data from Google Trends via DataForSEO API",
    tags=[env_args["env"], "externally_triggered"],
    doc_md=dag_args.load_docs(__file__),
    params=dag_args.load_config(scope="global"),
) as dag:
    start = EmptyOperator(task_id="start")

    data_export_end = data_export.register(start=start)
    instance_tasks_end = instance_tasks.register(start=data_export_end)
    data_load_end = data_load.register(start=instance_tasks_end)

    end = EmptyOperator(task_id="end")
    data_load_end >> end


if __name__ == "__main__":
    dag.test(variable_file_path="variables.yaml")
