import os

import pendulum

# Importing the necessary modules to run the DAG.
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from common.utils import dag_args

path = f"{os.path.dirname(os.path.realpath(__file__))}"
DAG_ID = f"{path.split('/')[-2]}-{path.split('/')[-1]}"

env_args = dag_args.make_env_args()
default_args=dag_args.make_default_args(
    start_date=pendulum.datetime(2022, 9, 20, tz="Europe/London"),
    retries=3,
)

with DAG(
    DAG_ID,
    default_args=default_args,
    description="Spin up the development Composer environment",
    schedule_interval=env_args["schedule_interval"],  # 08:05:00 Mon - Fri
    tags=[env_args["env"], "daily"],
) as setup:
    trigger_task = BashOperator(
        task_id="trigger_test-user-group-alerts",
        bash_command=f"gcloud composer environments run dev "
        "--project storage-dev-olvin-com "
        "--location europe-west1 "
        "--impersonate-service-account cross-project-worker-dev@storage-dev-olvin-com.iam.gserviceaccount.com "
        f'dags trigger -- -e "{{{{ ds }}}} {pendulum.now().time()}" test-user-group-alerts '
        "|| true ",  # this line is needed due to gcloud bug.
    )