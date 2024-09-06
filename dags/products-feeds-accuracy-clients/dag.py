import os
import pendulum
from datetime import datetime
from importlib.machinery import SourceFileLoader

from airflow import DAG
from airflow.operators.empty import EmptyOperator

from common.utils import dag_args, callbacks, slack_users

folder_path = f"{os.path.dirname(os.path.realpath(__file__))}"
DAG_ID = folder_path.split("/")[-1]

common = SourceFileLoader(
    "common", f"{folder_path}/steps/common.py"
).load_module()

alphamap = SourceFileLoader(
    "alphamap", f"{folder_path}/steps/alphamap.py"
).load_module()

man_group = SourceFileLoader(
    "man_group", f"{folder_path}/steps/man_group.py"
).load_module()

visionworks = SourceFileLoader(
    "visionworks", f"{folder_path}/steps/visionworks.py"
).load_module()

broadbay = SourceFileLoader(
    "broadbay", f"{folder_path}/steps/broadbay.py"
).load_module()

env_args = dag_args.make_env_args(
    schedule_interval="0 10 * * 1"
)
default_args=dag_args.make_default_args(
    start_date=datetime(2022, 9, 16),
    depends_on_past=True,
    wait_for_downstream=True,
    retries=3,
    on_failure_callback=callbacks.task_fail_slack_alert(
        slack_users.MATIAS, channel=env_args["env"]
    ),
)

with DAG(
    DAG_ID,
    default_args=default_args,
    schedule_interval=env_args["schedule_interval"],
    params=dag_args.load_config(__file__),
    doc_md=dag_args.load_docs(__file__),
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    common_end = common.register(dag=dag,start=start)

    alphamap_end = alphamap.register(start=common_end, dag=dag, env=env_args["env"])
    man_group_end = man_group.register(start=common_end, dag=dag, env=env_args["env"])
    visionworks_end = visionworks.register(start=common_end, dag=dag, env=env_args["env"])
    broadbay_end = broadbay.register(start=common_end, dag=dag, env=env_args["env"])
    
    [alphamap_end, man_group_end, visionworks_end, broadbay_end] >> end
