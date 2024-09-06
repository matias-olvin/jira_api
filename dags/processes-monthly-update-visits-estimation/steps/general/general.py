"""
DAG ID: real_visits
"""
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from common.utils.callbacks import check_dashboard_slack_alert
from common.operators.exceptions import MarkSuccessOperator


def register(dag, start):
    """Register tasks on the dag.

    Args:
        dag (airflow.models.DAG): DAG instance to register tasks on.
        start (airflow.models.TaskInstance): Task instance all tasks will
            registered downstream from.

    Returns:
        airflow.models.TaskInstance: The last task node in this section.

    """

    query_poi_classes = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="query_poi_classes",
        configuration={
            "query": {
                "query": "{% include './bigquery/general/poi_class.sql' %}",
                "useLegacySql": "False",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id |  replace('.','-') }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )

    mark_success_general = MarkSuccessOperator(
        task_id="mark_success_general",
        on_failure_callback=check_dashboard_slack_alert(
            "U03BANPLXJR", message="Check poi class/groups are properly generated."
        ),
        dag=dag,
    )

    start >> query_poi_classes >> mark_success_general

    return mark_success_general
