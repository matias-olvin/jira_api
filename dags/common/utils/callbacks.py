from __future__ import annotations

from datetime import date
from typing import Any, Dict

from airflow.hooks.base_hook import BaseHook
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook


def check_google_sheet_slack_alert(
    *users: str, spreadsheet_id: str = None, message: str = ""
) -> None:
    """
    Creates a function for sending a Slack alert about a Google Spreadsheet task,
    including relevant links and user mentions.

    Parameters:
    users (str): A variable number of user IDs to be mentioned in the Slack alert.
    spreadsheet_id (str, optional): The ID of the Google Spreadsheet. If provided, a link to
                                    the spreadsheet is included in the alert. Defaults to None.
    message (str, optional): A custom message to be included in the alert. Defaults to an empty string.

    Returns:
    function: A closure that, when called with an Airflow context, sends a Slack alert using
              the provided parameters and details from the context.
    """
    connection = "slack_notifications"
    user_msg = ":hourglass_flowing_sand:"

    for user_id in users:
        if user_id.startswith("<"):
            user_msg += f" {user_id}"
        else:
            user_msg += f" <@{user_id}>"

    user_msg += " Mark Task Success"

    spreadsheet_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit"

    spreadsheet_link = (
        f":mag_right: <{spreadsheet_url}|*GoogleSheet*> {message}"
        if spreadsheet_id is not None
        else ":question: No Spreadsheet ID Provided"
    )

    def submit_alert_user(context):
        slack_webhook_token = BaseHook.get_connection(connection).password
        slack_msg = (
            f"{user_msg}\n"
            f"{spreadsheet_link}\n"
            f"*Task*: {context['task_instance'].task_id}\n"
            f"*Dag*: {context['task_instance'].dag_id}\n"
            f"*Execution Time*: {context['execution_date']}\n"
            f"*Log Url*: {context['task_instance'].log_url}"
        )
        submit_alert = SlackWebhookHook(
            http_conn_id=connection,
            webhook_token=slack_webhook_token,
            message=slack_msg,
            username="airflow",
        )
        return submit_alert.execute()

    return submit_alert_user


def check_confluence_slack_alert(
    *users: str, confluence_page_link: str = None, message: str = ""
) -> None:
    """
    The function sends a slack notification alerting users to check the relevant confluence page

    Args:
      *users (str): Slack ID for user(s) to alert.
      confluence_page_link (str): A string representing the link to the relevant confluence page.
      message (str): customer message to include in the slack notififcation

    Returns:
      Sends a Slack alert message to a specified channel or user with information about
      the task instance, DAG, execution time, log URL, and a link to a documentation page in
      Confluence.
    """
    connection = "slack_notifications"
    user_msg = ":hourglass_flowing_sand:"

    for user_id in users:
        if user_id.startswith("<"):
            user_msg += f" {user_id}"
        else:
            user_msg += f" <@{user_id}>"

    user_msg += " Mark Task Success"

    confluence_link = (
        f":scroll: <{confluence_page_link}|*ConfluencePage*> {message}"
        if confluence_page_link is not None
        else ":question: No Confluence Page Link Provided"
    )

    def submit_alert_user(context):
        slack_webhook_token = BaseHook.get_connection(connection).password
        slack_msg = (
            f"{user_msg}\n"
            f"{confluence_link}\n"
            f"*Task*: {context['task_instance'].task_id}\n"
            f"*Dag*: {context['task_instance'].dag_id}\n"
            f"*Execution Time*: {context['execution_date']}\n"
            f"*Log Url*: {context['task_instance'].log_url}"
        )
        submit_alert = SlackWebhookHook(
            http_conn_id=connection,
            webhook_token=slack_webhook_token,
            message=slack_msg,
            username="airflow",
        )
        return submit_alert.execute()

    return submit_alert_user


def task_fail_slack_alert(*users: str, channel: str = "dev") -> None:
    if channel == "dev":
        connection = "slack_notifications"
    elif channel == "prod":
        connection = "slack_notifications_verification"
    date_today = date.today()
    if date_today.month == 10 and date_today.day >= 24:
        emoji = ":skull:"
    else:
        emoji = ":red_circle:"
    user_msg = "Task Failed."

    for user_id in users:
        if user_id.startswith("<"):
            user_msg += f" {user_id}"
        else:
            user_msg += f" <@{user_id}>"

    def submit_alert_user(context: Dict[str, Any]) -> None:
        slack_webhook_token = BaseHook.get_connection(connection).password
        slack_msg = f"""
{emoji} {user_msg}
*Task*: {context['task_instance'].task_id}  
*Dag*: {context['task_instance'].dag_id} 
*Execution Time*: {context['execution_date']}  
*Log Url*: {context['task_instance'].log_url} 
        """
        submit_alert = SlackWebhookHook(
            http_conn_id=connection,
            webhook_token=slack_webhook_token,
            message=slack_msg,
            username="airflow",
        )
        return submit_alert.execute()

    return submit_alert_user


def task_retry_slack_alert(*users: str, channel: str = "dev") -> None:
    if channel == "dev":
        connection = "slack_notifications"
    elif channel == "prod":
        connection = "slack_notifications_verification"
    date_today = date.today()
    if date_today.month == 10 and date_today.day >= 24:
        emoji = ":jack_o_lantern:"
    else:
        emoji = ":large_orange_circle:"
    user_msg = "Retrying Task."

    for user_id in users:
        if user_id.startswith("<"):
            user_msg += f" {user_id}"
        else:
            user_msg += f" <@{user_id}>"

    def submit_alert_user(context: Dict[str, Any]) -> None:
        slack_webhook_token = BaseHook.get_connection(connection).password
        slack_msg = f"""
{emoji} {user_msg}
*Task*: {context['task_instance'].task_id}  
*Dag*: {context['task_instance'].dag_id} 
*Execution Time*: {context['execution_date']}  
*Log Url*: {context['task_instance'].log_url} 
        """
        submit_alert = SlackWebhookHook(
            http_conn_id="slack_notifications",
            webhook_token=slack_webhook_token,
            message=slack_msg,
            username="airflow",
        )
        return submit_alert.execute()

    return submit_alert_user


def pipeline_end_slack_alert(*users: str, channel: str = "dev") -> None:
    if channel == "dev":
        connection = "slack_notifications"
    elif channel == "prod":
        connection = "slack_notifications_verification"
    date_today = date.today()
    if date_today.month == 10 and date_today.day >= 24:
        emoji = ":smiling_imp:"
    else:
        emoji = ":large_green_circle:"
    user_msg = "Pipeline Ended."

    for user_id in users:
        if user_id.startswith("<"):
            user_msg += f" {user_id}"
        else:
            user_msg += f" <@{user_id}>"

    def submit_alert_user(context: Dict[str, Any]) -> None:
        slack_webhook_token = BaseHook.get_connection(connection).password
        slack_msg = f"""
{emoji} {user_msg}
*Dag*: {context['task_instance'].dag_id} 
*Execution Time*: {context['execution_date']}  
        """
        submit_alert = SlackWebhookHook(
            http_conn_id="slack_notifications",
            webhook_token=slack_webhook_token,
            message=slack_msg,
            username="airflow",
        )
        return submit_alert.execute()

    return submit_alert_user


def task_end_custom_slack_alert(
    *users: str, channel: str = "dev", msg_title: str = ""
) -> None:
    if channel == "dev":
        connection = "slack_notifications"
    elif channel == "prod":
        connection = "slack_notifications_verification"
    emoji = ":bulb:"
    user_msg = msg_title

    for user_id in users:
        if user_id.startswith("<"):
            user_msg += f" {user_id}"
        else:
            user_msg += f" <@{user_id}>"

    def submit_alert_user(context):
        slack_webhook_token = BaseHook.get_connection(connection).password
        slack_msg = f"""
{emoji} {user_msg}
*Dag*: {context['task_instance'].dag_id} 
*Execution Time*: {context['execution_date']}  
*Log Url*: {context['task_instance'].log_url} 
        """
        submit_alert = SlackWebhookHook(
            http_conn_id="slack_notifications",
            webhook_token=slack_webhook_token,
            message=slack_msg,
            username="airflow",
        )
        return submit_alert.execute()

    return submit_alert_user


def check_dashboard_slack_alert(
    *users: str, data_studio_link: str = None, message: str = ""
) -> None:
    """
    This function creates a Slack alert message with information about a task instance
    and a link to a dashboard.

    Args:
      data_studio_link (str): A string representing the link to a Data Studio dashboard.
      *users (str): Slack ID for user(s) to alert.

    Returns:
      Sends a Slack alert message to a specified channel or user with information about
      the task instance, DAG, execution time, log URL, and a link to a dashboard in
      Data Studio.
    """
    connection = "slack_notifications"
    user_msg = ":hourglass_flowing_sand:"

    for user_id in users:
        if user_id.startswith("<"):
            user_msg += f" {user_id}"
        else:
            user_msg += f" <@{user_id}>"

    user_msg += " Mark Task Success"

    dashboard_link = (
        f":bar_chart: <{data_studio_link}|*Dashboard*> {message}"
        if data_studio_link is not None
        else ":bar_chart: No Dashboard Link Provided"
    )

    def submit_alert_user(context):
        slack_webhook_token = BaseHook.get_connection(connection).password
        slack_msg = (
            f"{user_msg}\n"
            f"{dashboard_link}\n"
            f"*Task*: {context['task_instance'].task_id}\n"
            f"*Dag*: {context['task_instance'].dag_id}\n"
            f"*Execution Time*: {context['execution_date']}\n"
            f"*Log Url*: {context['task_instance'].log_url}"
        )
        submit_alert = SlackWebhookHook(
            http_conn_id=connection,
            webhook_token=slack_webhook_token,
            message=slack_msg,
            username="airflow",
        )
        return submit_alert.execute()

    return submit_alert_user
