"""
Utility functions for building DAGs
"""
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, Optional

import yaml
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook


def make_default_args(**kwargs):
    """Make a new default argument dictionary from base arguments and
    kwargs.

    Args:
        kwargs (optional): Additional default arguments to use.
    """
    return {
        "owner": "airflow",
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        **kwargs,
    }


def make_env_args(folder: str, dag_id: str, schedule_interval: str = None) -> Dict:
    """
    > It takes a folder name, a DAG ID, and a schedule interval, and returns a dictionary of DAG
    arguments

    Args:
      folder (str): The folder where the DAG is located.
      dag_id (str): The name of the DAG.
      schedule_interval (str): The schedule interval for the DAG.

    Returns:
      A dictionary with the dag_id, schedule_interval, and env.
    """
    dag_args = {
        "dag_id": dag_id,
        "schedule_interval": schedule_interval,
        "env": "prod",
    }

    prod = "prod" in Variable.get("env_project")

    if not prod:
        dag_args["schedule_interval"] = None
        dag_args["env"] = "dev"

        if "-" in folder:
            feature = folder.split("-")[-1]
            dag_args["dag_id"] = f"{dag_id}.{feature}"

    return dag_args


def load_config(
    caller_path: str = None, path: str = "config.yaml", scope: str = "local"
) -> Dict:
    """
    It loads a yaml file relative to the caller file

    Args:
      caller_path: Absolute path of the caller file. Typically
        passed with `__file__`.
      path: Relative path to the configuration file from
        `caller_path`. Defaults to `config.yaml`.
      scope: local or global. Defaults to local

    Returns:
      The parsed configuration dictionary.
    """
    if scope == "global":
        with open("/home/airflow/gcs/dags/global-config.yaml", "r") as config:
            return yaml.load(config, Loader=yaml.FullLoader)

    elif scope == "local":
        caller_dir = Path(caller_path).parent
        with Path(caller_dir, path).open() as config:
            return yaml.load(config, Loader=yaml.FullLoader)


def load_docs(caller_path, path="README.md"):
    """Load a markdown file as a string relative from the caller_path.

    Args:
        caller_path (string): Absolute path of the caller file. Typically
            passed with `__file__`.
        path (string, optional): Relative path to the docs file from
            `caller_path`. Defaults to `README.md`.

    Returns:
        Dict: The loaded markdown string.
    """
    caller_dir = Path(caller_path).parent

    with Path(caller_dir, path).open() as file:
        return file.read()


def task_fail_slack_alert(*users, channel="dev"):
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
        user_msg = f"{user_msg} <@{user_id}>"

    def submit_alert_user(context):
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


def task_retry_slack_alert(*users, channel="dev"):
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
        user_msg = f"{user_msg} <@{user_id}>"

    def submit_alert_user(context):
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


def pipeline_end_slack_alert(*users, channel="dev"):
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
        user_msg = f"{user_msg} <@{user_id}>"

    def submit_alert_user(context):
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


def task_end_custom_slack_alert(*users, channel="dev", msg_title=""):
    if channel == "dev":
        connection = "slack_notifications"
    elif channel == "prod":
        connection = "slack_notifications_verification"
    emoji = ":bulb:"
    user_msg = msg_title
    for user_id in users:
        user_msg = f"{user_msg} <@{user_id}>"

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
