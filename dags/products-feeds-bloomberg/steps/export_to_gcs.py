"""
DAG ID: boomberg_feed
"""
from datetime import timedelta

import pendulum
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from airflow.sensors.date_time_sensor import DateTimeSensor

SOURCE_DATA_FOLDER = "gs://{{ params['bloomberg_bucket'] }}"


def send_empty_success_file(dag, **context):
    gcs_hook = GCSHook()
    bucket_name = f"{dag.params['bloomberg_bucket']}"
    object_name = f"{ context['execution_date'].strftime('%Y')}/{ context['execution_date'].strftime('%m')}/{ context['execution_date'].strftime('%d')}/_SUCCESS"
    file_name = "_SUCCESS"
    # Create an empty file locally
    with open(file_name, "w") as f:
        pass

    # Upload the empty file to GCS
    gcs_hook.upload(
        bucket_name=bucket_name,
        object_name=object_name,
        mime_type="application/octet-stream",
        filename=file_name,
        gzip=False,
    )

    return


def register(dag, start):
    """Register tasks on the dag.

    Args:
        dag (airflow.models.DAG): DAG instance to register tasks on.
        start (airflow.models.TaskInstance): Task instance all tasks will
            registered downstream from.

    Returns:
        airflow.models.TaskInstance: The last task node in this section.
    """
    export_bloomberg_feed = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="export_bloomberg_feed",
        project_id=f"{dag.params['project']}",
        configuration={
            "extract": {
                "destinationUri": (
                    (
                        f"{SOURCE_DATA_FOLDER}/"
                        "{{ execution_date.strftime('%Y')  }}/"
                        "{{ execution_date.strftime('%m')  }}/"
                        "{{ execution_date.strftime('%d')  }}/"
                        "*.parquet"
                    )
                ),
                "destinationFormat": "PARQUET",
                "sourceTable": {
                    "projectId": "{{ params['project'] }}",
                    "datasetId": "{{ params['bloomberg_dataset'] }}",
                    "tableId": "{{ execution_date.subtract(days=var.value.latency_days_visits|int).strftime('%Y') }}${{ execution_date.subtract(days=var.value.latency_days_visits|int).strftime('%Y%m%d') }}",
                },
            },
            "labels": {
                "pipeline": "{{ dag.dag_id }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        wait_for_downstream=False,
    )
    send_empty_success_file_task = PythonOperator(
        task_id="send_empty_success_file",
        python_callable=send_empty_success_file,
        op_kwargs={
            "dag": dag,
        },
        provide_context=True,
        dag=dag,
    )
    delete_from_bloomberg_feed = BashOperator(
        task_id="delete_from_bloomberg_feed",
        bash_command="bq rm -f '{{ params['project'] }}:{{ params['bloomberg_dataset'] }}.{{ execution_date.subtract(days=30).strftime('%Y') }}\
            ${{ execution_date.subtract(days=30).strftime('%Y%m%d') }}'",
        dag=dag,
    )
    (
        start
        >> export_bloomberg_feed
        >> send_empty_success_file_task
        >> delete_from_bloomberg_feed
    )

    # Notifications for Tamoco in case there is a delay
    def time_check_short_circuit(**context):
        return pendulum.now("UTC") > (
            context["execution_date"].replace(hour=13, minute=50) + timedelta(days=1)
        )

    in_time_short_circuit = ShortCircuitOperator(
        task_id="in_time_short_circuit",
        dag=dag,
        python_callable=time_check_short_circuit,
        provide_context=True,
        depends_on_past=False,
        wait_for_downstream=False,
    )
    export_bloomberg_feed >> in_time_short_circuit

    notify_users = [
        "U05S63PAM7G",  # Dirie
        "U05SA1PNKAR",  # Henry
        "U05SAP1AZ5K",  # Pablo
        "U02KJ556S1H",  # Kartikey
    ]

    def submit_alert_export(**context):
        user_msg = f""":large_green_circle: *Data export finished*
Export date: {context['execution_date'].strftime('%Y-%m-%d')}
Data date: {context['execution_date'].subtract(days=int(Variable.get('latency_days_visits'))).strftime('%Y-%m-%d')}
"""
        for user_id in notify_users:
            user_msg = f"{user_msg} <@{user_id}>"
        slack_webhook_token = BaseHook.get_connection(
            "slack_notifications_gcp_feed"
        ).password
        submit_alert = SlackWebhookHook(
            http_conn_id="slack_notifications_gcp_feed",
            webhook_token=slack_webhook_token,
            message=user_msg,
            username="airflow",
        )
        return submit_alert.execute()

    notify_export = PythonOperator(
        task_id="notify_export",
        python_callable=submit_alert_export,
        provide_context=True,
        dag=dag,
        depends_on_past=False,
        wait_for_downstream=False,
    )
    in_time_short_circuit >> notify_export

    export_deadline_sensor = DateTimeSensor(
        task_id="export_deadline_sensor",
        dag=dag,
        poke_interval=60 * 10,
        timeout=60 * 60 * 24,
        mode="reschedule",
        target_time="{{ execution_date.add(days=1).replace(hour=13, minute=50) }}",
        depends_on_past=False,
        wait_for_downstream=False,
    )

    def submit_alert_delay(**context):
        user_msg = f""":octagonal_sign: *Data export delayed*
Export date: {context['execution_date'].strftime('%Y-%m-%d')}
Data date: {context['execution_date'].subtract(days=int(Variable.get('latency_days_visits'))).strftime('%Y-%m-%d')}
"""
        for user_id in notify_users:
            user_msg = f"{user_msg} <@{user_id}>"
        slack_webhook_token = BaseHook.get_connection(
            "slack_notifications_gcp_feed"
        ).password
        submit_alert = SlackWebhookHook(
            http_conn_id="slack_notifications_gcp_feed",
            webhook_token=slack_webhook_token,
            message=user_msg,
            username="airflow",
        )
        return submit_alert.execute()

    notify_delay = PythonOperator(
        task_id="notify_delay",
        python_callable=submit_alert_delay,
        provide_context=True,
        dag=dag,
        depends_on_past=False,
        wait_for_downstream=False,
        trigger_rule="one_success",
    )
    [export_deadline_sensor, in_time_short_circuit] >> notify_delay

    return export_bloomberg_feed
