from datetime import datetime

from airflow.operators.python_operator import PythonOperator
from common.operators.bigquery import OlvinBigQueryOperator


def register(dag, start):
    def extract_month_year(date: str) -> str:
        """
        This Python function extracts the month and year from a given date string in the format of
        "YYYY-MM-DD" and returns it in the format of "Month Year".

        Args:
          date (str): The input parameter `date` is a string representing a date in the format
        "YYYY-MM-DD".

        Returns:
          The function `extract_month_year` takes a string argument `date` in the format of "YYYY-MM-DD"
        and returns a string in the format of "Month Year" (e.g. "January 2022") by using the `datetime`
        module to convert the input date string into a `datetime` object and then formatting it using
        the `strftime` method.
        """
        date_obj = datetime.strptime(date, "%Y-%m-%d")
        return date_obj.strftime("%B %Y")

    def submit_alert_user(**context):
        """
        This function sends a Slack notification with a message and a link to a Google Sheets document.

        Returns:
          the result of executing the SlackWebhookHook object's execute() method, which sends a message
        to a Slack channel using the webhook token and message provided.
        """
        from airflow.hooks.base_hook import BaseHook
        from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

        slack_webhook_token = BaseHook.get_connection(
            "slack_notifications_sales"
        ).password
        slack_msg = (
            "*Monthly Metrics for "
            f"{extract_month_year(context['templates_dict']['run_date'])} "
            "ready* <!here> \n"
            ":point_right:Results available in "
            "<https://docs.google.com/spreadsheets/d/1oh47mc08jVBX9q5EL9_"
            "lyibTsIv5mmT3WWFz5S021gs/edit#gid=1489288217|*Sheets*>"
        )
        submit_alert = SlackWebhookHook(
            http_conn_id="slack_notifications",
            webhook_token=slack_webhook_token,
            message=slack_msg,
            username="airflow",
        )
        return submit_alert.execute()

    dylans_table = OlvinBigQueryOperator(
task_id="dylans_table", query="{% include './bigquery/dylan_alert.sql' %}"
    )
    start >> dylans_table

    notify_export = PythonOperator(
        task_id="notify_export",
        python_callable=submit_alert_user,
        templates_dict={
            "run_date": "{{ ds }}",
        },
        provide_context=True,
        dag=dag,
    )
    dylans_table >> notify_export

    return notify_export
