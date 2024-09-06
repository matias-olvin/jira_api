import os
from importlib.machinery import SourceFileLoader

import pendulum
from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from common.utils import callbacks, dag_args, slack_users

folder_path = f"{os.path.dirname(os.path.realpath(__file__))}"
DAG_ID = folder_path.split("/")[-1]

type2 = SourceFileLoader("type2", f"{folder_path}/steps/type2.py").load_module()

type1 = SourceFileLoader("type1", f"{folder_path}/steps/type1.py").load_module()

sample = SourceFileLoader("sample", f"{folder_path}/steps/sample.py").load_module()

env_args = dag_args.make_env_args(schedule_interval="0 6 1 * *")
default_args = dag_args.make_default_args(
    start_date=pendulum.datetime(2022, 9, 16),
    depends_on_past=True,
    wait_for_downstream=True,
    retries=3,
    on_failure_callback=callbacks.task_fail_slack_alert(
        slack_users.MATIAS, slack_users.KARTIKEY, channel=env_args["env"]
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

    sample_end = sample.register(start=start, dag=dag)

    type1_end = type1.register(start=sample_end, dag=dag)

    type2_end = type2.register(start=type1_end, dag=dag)

    type2_end >> end
