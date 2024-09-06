import os
from datetime import datetime, timedelta
from importlib.machinery import SourceFileLoader

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from common.utils import callbacks, dag_args

path = f"{os.path.dirname(os.path.realpath(__file__))}"
DAG_ID = path.split("/")[-1]
steps_path = f"{path}/steps/"

data_load_bq = SourceFileLoader(
    "data_load_bq", f"{steps_path}/data_load_bq.py"
).load_module()
inference_job = SourceFileLoader(
    "inference_job", f"{steps_path}/inference_job.py"
).load_module()
initial_delete = SourceFileLoader(
    "initial_delete", f"{steps_path}/initial_delete.py"
).load_module()

env_args = dag_args.make_env_args(
    schedule_interval="0 0 1 1/3 *",
)

default_args = dag_args.make_default_args(
    start_date=datetime(2022, 9, 14),
    retries=3,
    on_failure_callback=callbacks.task_fail_slack_alert(
        "U02KJ556S1H", channel=env_args["env"]
    ),
)

with DAG(
    DAG_ID,
    default_args=default_args,
    concurrency=8,
    schedule_interval=env_args["schedule_interval"],
    dagrun_timeout=timedelta(hours=12),
    params=dag_args.load_config(scope="global"),
    doc_md=dag_args.load_docs(__file__),
) as dag:

    start = EmptyOperator(task_id="start")

    initial_delete_end = initial_delete.register(start=start, dag=dag)

    inference_job_end = inference_job.register(start=initial_delete_end, dag=dag)

    data_load_bq_end = data_load_bq.register(start=inference_job_end, dag=dag)

    end = EmptyOperator(
        task_id="end",
        on_success_callback=callbacks.pipeline_end_slack_alert(
            "U02KJ556S1H", channel=env_args["env"]
        ),
    )

    data_load_bq_end >> end
