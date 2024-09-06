from __future__ import annotations

import pendulum
from airflow.hooks.base_hook import BaseHook
from airflow.models import DAG, TaskInstance, Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from airflow.utils.task_group import TaskGroup
from common.operators.bigquery import OlvinBigQueryOperator
from common.operators.exceptions import MarkSuccessOperator


def postgres_slack_alert(*users: str, progress: str, job: str, table=""):
    """
    The above function is used to send a slack notification to the user

    Args:
      progress (str): This is the progress of the job. It can be start, end, failed, or completed.
      job (str): The name of the job.
      table (str): The name of the table that is being processed.

    Returns:
      The return value is the response from the Slack API.
    """
    config_params = {
        "start": {
            "emoji": ":hourglass_flowing_sand:",
            "msg_title": f"{table} - {job} Required",
        },
        "end": {"emoji": ":checkered_flag:", "msg_title": f"{table} - {job} Completed"},
        "failed": {"emoji": ":skull:", "msg_title": f"{table} - {job} Failed"},
        "completed": {"emoji": ":partying_face:", "msg_title": f"{job} Completed!"},
    }
    emoji = config_params.get(progress).get("emoji")
    msg_title = config_params.get(progress).get("msg_title")

    # Getting the configuration for the progress that is being passed to the function.
    def submit_alert_config(*args, **kwargs):
        # Getting the slack token from the connection.
        slack_webhook_token = BaseHook.get_connection(
            "slack_notifications_postgres"
        ).password
        # Creating a message to be sent to Slack.
        slack_msg = ":almanac:"
        for user_id in users:
            slack_msg += f" <@{user_id}>"
        slack_msg += f" {msg_title} {emoji}"

        submit_alert = SlackWebhookHook(
            http_conn_id="slack_notifications_postgres",
            webhook_token=slack_webhook_token,
            message=slack_msg,
            username="airflow",
        )

        # Returning the response from the Slack API.
        submit_alert.execute()

    return submit_alert_config


def register(start: TaskInstance, dag: DAG) -> TaskGroup:
    def prev_month() -> str:
        date_obj = pendulum.parse(Variable.get("monthly_update"))
        prev_month_date = (date_obj - pendulum.duration(months=1)).replace(day=1)
        return prev_month_date.strftime("%Y-%m-%d")

    def prev_data_uri() -> str:
        prev_date = prev_month()
        year, month, _ = prev_date.split("-")
        return f"gs://postgres-final-database-tables/{dag.params['SGPlaceDailyVisitsRaw_table']}/{year}/{int(month)}/*.csv.gz"

    with TaskGroup(group_id="postgres_version") as group:

        postgres_version_notification = EmptyOperator(
            task_id="postgres_version_notification",
            on_success_callback=postgres_slack_alert(
                "U7NUR99LY",
                "U03NLSDFB1B",  # Matt, Furkan
                progress="start",
                job="Review of start_date for current version and end_date for previous version",
                table="Version",
            ),
        )

        check_version = MarkSuccessOperator(task_id="check_version", dag=dag)

        query_Version_table = OlvinBigQueryOperator(
            task_id="query_Version_table",
            query='{% include "./bigquery/postgres_version/Version.sql" %}',
        )

        load_SGPlaceDailyVisitsRaw_prev = OlvinBigQueryOperator(
            task_id="load_SGPlaceDailyVisitsRaw_prev",
            query='{% with uris="'
            f"{prev_data_uri()}"
            '"%}{% include "./bigquery/postgres_version/load_SGPlaceDailyVisitsRaw_prev.sql" %}{% endwith %}',
        )

        query_PlaceCompatibility_table = OlvinBigQueryOperator(
            task_id="query_PlaceCompatibility_table",
            query='{% include "./bigquery/postgres_version/PlaceCompatibility.sql" %}',
        )

        drop_SGPlaceDailyVisitsRaw_prev = OlvinBigQueryOperator(
            task_id="drop_SGPlaceDailyVisitsRaw_prev",
            query='{% include "./bigquery/postgres_version/drop_SGPlaceDailyVisitsRaw_prev.sql" %}',
        )

        copy_Version_table_from_postgres_batch_to_rt = OlvinBigQueryOperator(
            task_id="copy_Version_table_from_postgres_batch_to_rt",
            query='{% include "./bigquery/postgres_version/copy_Version_table_from_postgres_batch_to_rt.sql" %}',
        )

        (
            start
            >> postgres_version_notification
            >> check_version
            >> query_Version_table
            >> load_SGPlaceDailyVisitsRaw_prev
            >> query_PlaceCompatibility_table
            >> drop_SGPlaceDailyVisitsRaw_prev
            >> copy_Version_table_from_postgres_batch_to_rt
        )

    return group
