import airflow
import pendulum
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python import PythonOperator
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from common.utils import dag_args

default_args = dag_args.make_default_args(
    start_date=pendulum.datetime(2023, 1, 1, tz="Europe/London"),
    retries=2,
)

with airflow.DAG(
    "test-user-group-alerts",
    default_args=default_args,
    schedule_interval=None,
):
    def teardown_slack_alert():
        """
        This function will send a Slack message to the channel 15 minutes before the
        Composer environment is scheduled to shutdown

        Returns:
          The return value is the response from the Slack API.
        """
        slack_webhook_token = BaseHook.get_connection("slack_notifications").password
        slack_msg = ":eyes: testing this thing -> <!subteam^S0679T5QLUU|de_gcp> <!subteam^S0674FX5PDK|ds_gcp> <!subteam^S067WEL1AFJ|gcp_>"
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