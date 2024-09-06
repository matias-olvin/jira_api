"""
DAG ID: sg_agg_stats
"""
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from common.utils import slack_users


def register(dag, start, mode):
    """Register tasks on the dag.

    Args:
        dag (airflow.models.DAG): DAG instance to register tasks on.
        start (airflow.models.TaskInstance): Task instance all tasks will
            registered downstream from.

    Returns:
        airflow.models.TaskInstance: The last task node in this section.
    """
    query_monthly_stats = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="query_monthly_stats",
        project_id=dag.params["project"],
        retries=1,
        configuration={
            "query": {
                "query": "{% include './bigquery/monthly_stats.sql' %}",
                "useLegacySql": "False",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )

    if mode == "update":
        export_monthly_stats = BigQueryInsertJobOperator(
            task_id="export_monthly_stats",
            project_id=dag.params["project"],
            configuration={
                "extract": {
                    "destinationUri": (
                        (
                            "gs://{{ params['vhl_staging_bucket'] }}/{{ ds }}/monthly_stats/"
                            "activity_stats_{{ ds }}.csv"
                        )
                    ),
                    "printHeader": True,
                    "destinationFormat": "CSV",
                    "sourceTable": {
                        "projectId": "{{ params['project'] }}",
                        "datasetId": "{{ params['Activity_dataset'] }}",
                        "tableId": "{{ params['activity_stats_table'] }}",
                    },
                },
                "labels": {
                    "pipeline": "{{ dag.dag_id }}",
                    "task_id": "{{ task.task_id.lower()[:63] }}",
                },
            },
            dag=dag,
        )
    elif mode == "backfill":
        export_monthly_stats = BigQueryInsertJobOperator(
            task_id="export_monthly_stats",
            project_id=dag.params["project"],
            configuration={
                "extract": {
                    "destinationUri": (
                        (
                            "gs://{{ params['vhl_staging_bucket'] }}/backfill/monthly_stats/"
                            "activity_stats_backfill.csv"
                        )
                    ),
                    "printHeader": True,
                    "destinationFormat": "CSV",
                    "sourceTable": {
                        "projectId": "{{ params['project'] }}",
                        "datasetId": "{{ params['Activity_dataset'] }}",
                        "tableId": "{{ params['activity_stats_table'] }}",
                    },
                },
                "labels": {
                    "pipeline": "{{ dag.dag_id }}",
                    "task_id": "{{ task.task_id.lower()[:63] }}",
                },
            },
            dag=dag,
        )

    def submit_alert_user(**context):
        from airflow.hooks.base_hook import BaseHook
        from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

        user_msg = f"""*Monthly Activity Stats for {context['templates_dict']['run_date']} ready*
Results available at: \
    https://console.cloud.google.com/storage/browser/{dag.params['vhl_staging_bucket']}/{context['templates_dict']['run_date']}/monthly_stats/
"""
        for user_id in [slack_users.CARLOS, slack_users.IGNACIO, slack_users.MATIAS]:
            user_msg = f"{user_msg} {user_id}"
        slack_webhook_token = BaseHook.get_connection("slack_notifications").password
        slack_msg = f"""
:bulb: {user_msg}
*Dag*: {context.get('task_instance').dag_id}
        """
        submit_alert = SlackWebhookHook(
            http_conn_id="slack_notifications",
            webhook_token=slack_webhook_token,
            message=slack_msg,
            username="airflow",
        )
        return submit_alert.execute()

    if mode == "update":
        notify_export = PythonOperator(
            task_id="notify_export",
            python_callable=submit_alert_user,
            templates_dict={
                "run_date": "{{ ds }}",
            },
            provide_context=True,
            dag=dag,
        )
    elif mode == "backfill":
        notify_export = PythonOperator(
            task_id="notify_export",
            python_callable=submit_alert_user,
            templates_dict={
                "run_date": "backfill",
            },
            provide_context=True,
            dag=dag,
        )

    start >> query_monthly_stats >> export_monthly_stats >> notify_export

    return notify_export
