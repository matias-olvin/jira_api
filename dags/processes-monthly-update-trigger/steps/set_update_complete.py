from airflow.hooks.base_hook import BaseHook
from airflow.models import DAG, TaskInstance, Variable
from airflow.operators.python import PythonOperator
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from common.operators.bigquery import OlvinBigQueryOperator


def register(start: TaskInstance, dag: DAG) -> TaskInstance:
    """
    Register tasks on the dag.

    Args:
        dag (airflow.models.DAG): DAG instance to register tasks on.
        start (airflow.models.TaskInstance): Task instance all tasks will
            registered downstream from.

    Returns:
        airflow.models.TaskInstance: The last task node in this section.
    """

    def update_complete_alert():
        """
        This function is sending a slack message to two different channels when the monthly update is
        complete

        Returns:
          The SlackWebhookHook object is being returned.
        """
        # A dictionary that is converting the month number to the month name.
        month_conversion = {
            "01": "January",
            "02": "February",
            "03": "March",
            "04": "April",
            "05": "May",
            "06": "June",
            "07": "July",
            "08": "August",
            "09": "September",
            "10": "October",
            "11": "November",
            "12": "December",
        }
        # This is getting the month number from the monthly_update variable.
        month_num = Variable.get("monthly_update").split("-")[1]
        # This is a dictionary that is converting the month number to the month name.
        month_str = month_conversion[f"{month_num}"]
        slack_msg = f":almanac: :partying_face: <!here> {month_str} Update Complete!"
        # This is a list of the slack channels that the message will be sent to.
        channels = ["slack_notifications", "slack_notifications_database"]
        # Sending a slack message to two different channels.
        for channel in channels:
            # This is getting the password from the connection that is stored in Airflow.
            slack_webhook_token = BaseHook.get_connection(f"{channel}").password
            # Creating a SlackWebhookHook object.
            submit_alert = SlackWebhookHook(
                http_conn_id=f"{channel}",
                webhook_token=slack_webhook_token,
                message=slack_msg,
                username="airflow",
            )

            return submit_alert.execute()

    # Set complete to True for execution_date
    set_update_complete = OlvinBigQueryOperator(
        task_id="set_update_complete",
        query="{% include './bigquery/set_update_complete.sql' %}",
    )

    alert_update_complete = PythonOperator(
        task_id="alert_update_complete",
        python_callable=update_complete_alert,
    )

    start >> set_update_complete >> alert_update_complete

    return alert_update_complete
