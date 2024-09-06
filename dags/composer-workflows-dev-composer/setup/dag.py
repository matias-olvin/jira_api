import os

import pendulum

# Importing the necessary modules to run the DAG.
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from common.utils import dag_args

path = f"{os.path.dirname(os.path.realpath(__file__))}"
DAG_ID = f"{path.split('/')[-2]}-{path.split('/')[-1]}"

# Creating a DAG called `dev_composer_setup` with the following parameters:
# - `default_args`: The default arguments for the DAG.
# - `description`: A description of the DAG.
# - `schedule_interval`: The schedule interval for the DAG.
env_args = dag_args.make_env_args(
    schedule_interval="00 08 * * 1-5",
)
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
    doc_md=dag_args.load_docs(__file__),
) as setup:
    # BashOperators are run from a temporary location so a cd is necessary to ensure
    # relative paths in the Bash scripts aren't broken
    setup_bash_command = """
    cd ~/gcs/dags/composer-workflows-dev-composer/include/bash
    chmod u+x composer_setup.sh
    ./composer_setup.sh dev """  # Note that there is a space at the end of the above command after dev

    # Running the bash script `composer_setup.sh`
    setup_bash = BashOperator(
        task_id="composer_setup",
        bash_command=setup_bash_command,
    )