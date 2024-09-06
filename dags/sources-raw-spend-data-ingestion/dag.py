import os
from datetime import datetime
from importlib.machinery import SourceFileLoader

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from common.utils import callbacks, dag_args, slack_users

path = f"{os.path.dirname(os.path.realpath(__file__))}"
DAG_ID = path.split("/")[-1]
steps_path = f"{path}/steps"

load_sg_spend_data = SourceFileLoader(
    "load_sg_spend_data", f"{steps_path}/load_sg_spend_data.py"
).load_module()

env_args = dag_args.make_env_args(
    dag_id=DAG_ID,
    schedule_interval="0 0 21 * *" # monthly on 21st day
)

with DAG(
    env_args["dag_id"],
    default_args=dag_args.make_default_args(
        retries=3,
    ),
    description="DAG for ingesting spend data",
    start_date=datetime(2022, 9, 16),
    schedule_interval=env_args["schedule_interval"],
    on_failure_callback=callbacks.task_fail_slack_alert(
        slack_users.MATIAS,
    ),
    on_success_callback=callbacks.pipeline_end_slack_alert(
        slack_users.MATIAS,
    ),
    params=dag_args.load_config(__file__),
    doc_md=dag_args.load_docs(__file__),
) as spend_dag:
    start = DummyOperator(task_id="start", dag=spend_dag)
    # load latest spend data
    load_sg_spend_data_end = load_sg_spend_data.register(dag=spend_dag, start=start)


# with DAG(
#     dag_id="sg_spend_backfill",
#     default_args=utils.make_default_args(
#         depends_on_past=False,
#         retries=3,
#     ),
#     description="DAG for backfilling spend data",
#     start_date=datetime(2022, 8, 16),
#     schedule_interval='0 0 21 * *',    # monthly on 21st day
#     catchup=True,
#     on_failure_callback=utils.task_fail_slack_alert(
#         "U034YDXAD1R",
#     ),
#     on_success_callback=utils.pipeline_end_slack_alert(
#         "U034YDXAD1R",
#     ),
#     params=utils.load_config(__file__),
#     # doc_md=utils.load_docs(__file__),
# ) as spend_backfill_dag:

#     start=DummyOperator(
#         task_id="start",
#         dag=spend_backfill_dag
#     )
#     # load latest spend data
#     load_sg_spend_data_end = load_sg_spend_data.register(
#         dag=spend_backfill_dag,
#         start=start
#     )
