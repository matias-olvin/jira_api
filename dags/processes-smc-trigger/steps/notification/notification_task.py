from __future__ import annotations
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable, DAG, TaskInstance
from airflow.operators.python import PythonOperator
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from pendulum import parser
def register(start: TaskInstance, dag: DAG):
    """
    Register tasks on the dag.
    Args:
        dag (airflow.models.DAG): DAG instance to register tasks on.
        start (airflow.models.TaskInstance): Task instance all tasks will
            registered downstream from.
    Returns:
        airflow.models.TaskInstance: The last task node in this section.
    """
    def smc_trigger_complete_alert():
        """
        This function is sending a slack message to two different channels when the smc_trigger dag is
        complete
        Returns:
          The SlackWebhookHook object is being returned.
        """
        slack_msg = f":almanac: :partying_face: <!here> smc_trigger complete"
        # This is a list of the slack channels that the message will be sent to.
        channel = "slack_notifications"
        # Sending a slack message to two different channels.
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

    def set_airflow_var(key, val):
        Variable.set(f"{key}", f"{val}")

    set_prev_smc_end_date_to_curr_smc_end_date = PythonOperator(
        task_id="set_prev_smc_end_date_to_curr_smc_end_date",
        python_callable=set_airflow_var,
        op_kwargs={"key": "smc_end_date_prev", "val": f"{Variable.get('smc_end_date')}"},
        dag=dag,
    )

    alert_smc_trigger_complete = PythonOperator(
        task_id="smc_trigger_complete_alert",
        python_callable=smc_trigger_complete_alert,
    )

    start >> set_prev_smc_end_date_to_curr_smc_end_date >> alert_smc_trigger_complete

    return alert_smc_trigger_complete