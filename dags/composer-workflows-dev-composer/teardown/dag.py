import os

import pendulum

# Importing the necessary modules to run the DAG.
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from common.utils import dag_args

path = f"{os.path.dirname(os.path.realpath(__file__))}"
DAG_ID = f"{path.split('/')[-2]}-{path.split('/')[-1]}"

# Creating a DAG called `dev_composer_teardown` with the following parameters:
# - `default_args`: The default arguments for the DAG.
# - `description`: A description of the DAG.
# - `schedule_interval`: The schedule interval for the DAG.
env_args = dag_args.make_env_args(
    schedule_interval="00 19 * * 1-5",
)
default_args=dag_args.make_default_args(
    start_date=pendulum.datetime(2022, 9, 20, tz="Europe/London"),
    retries=3,
)

with DAG(
    DAG_ID,
    default_args=default_args,
    description="Tear down the development Composer environment",
    schedule_interval=env_args["schedule_interval"],  # 19:00:00 Mon - Fri
    tags=[env_args["env"], "daily"],
    doc_md=dag_args.load_docs(__file__),
) as teardown:

    def teardown_slack_alert():
        """
        This function will send a Slack message to the channel 15 minutes before the
        Composer environment is scheduled to shutdown

        Returns:
          The return value is the response from the Slack API.
        """
        slack_webhook_token = BaseHook.get_connection("slack_notifications").password
        slack_msg = """
:skull: Dev Composer will Shutdown in 15 minutes. <!here>
:arrow_right: <https://b8e2af37ff834fb0876a9b6b4e5faf5d-dot-europe-west1.composer.\
googleusercontent.com/tree?dag_id=dev_env_teardown|*Pause DAG*> to prevent teardown...
"""
        submit_alert = SlackWebhookHook(
            http_conn_id="slack_notifications",
            webhook_token=slack_webhook_token,
            message=slack_msg,
            username="airflow",
        )

        return submit_alert.execute()

    # Sending a Slack message to the channel 15 minutes before the
    # Composer environment is scheduled to shutdown
    alert = PythonOperator(
        task_id="alert",
        python_callable=teardown_slack_alert,
    )

    def wait_for_response(mins=15):
        """
        It waits for 15 minutes

        Args:
          mins: The number of minutes to wait before the script will run again. Defaults to 15
        """
        import time

        time.sleep(mins * 60)

    # Waiting for 15 minutes before running the next task.
    wait = PythonOperator(task_id="wait", python_callable=wait_for_response)

    # BashOperators are run from a temporary location so a cd is necessary to ensure
    # relative paths in the Bash scripts aren't broken
    bash_command = """
    cd ~/gcs/dags/composer-workflows-dev-composer/include/bash
    chmod u+x composer_teardown.sh
    ./composer_teardown.sh dev """  # Note that there is a space at the end of the above command after dev

    # Running a bash script called `composer_teardown.sh`
    bash = BashOperator(
        task_id="composer-teardown",
        bash_command=bash_command,
    )

    # Creating a dependency between the three tasks.
    alert >> wait >> bash
