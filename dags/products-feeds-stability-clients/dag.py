import os
from datetime import datetime
from importlib.machinery import SourceFileLoader

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from common.utils import callbacks, dag_args, slack_users

folder_path = f"{os.path.dirname(os.path.realpath(__file__))}"
DAG_ID = folder_path.split("/")[-1]

common = SourceFileLoader("common", f"{folder_path}/steps/common.py").load_module()

dewey = SourceFileLoader("dewey", f"{folder_path}/steps/dewey.py").load_module()
maiden_century = SourceFileLoader(
    "maiden_century", f"{folder_path}/steps/maiden_century.py"
).load_module()
quantcube = SourceFileLoader(
    "quantcube", f"{folder_path}/steps/quantcube.py"
).load_module()

env_args = dag_args.make_env_args(schedule_interval="0 10 * * 1")
default_args = dag_args.make_default_args(
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
    common_end = common.register(dag=dag, start=start)

    dewey_end = dewey.register(dag=dag, start=common_end)
    maiden_century_end = maiden_century.register(start=common_end, dag=dag)
    quantcube_end = quantcube.register(start=common_end, dag=dag)

    turn_on_requester_pays_gcs = BashOperator(
        task_id="turn_on_requester_pays_gcs",
        bash_command=f"gcloud storage buckets update "
        f"gs://{{{{ params['feeds_staging_gcs_bucket'] }}}} "
        f"--requester-pays --billing-project={{{{ params['data_feeds_project'] }}}}",
    )

    (
        [dewey_end, maiden_century_end, quantcube_end]
        >> turn_on_requester_pays_gcs
        >> end
    )
