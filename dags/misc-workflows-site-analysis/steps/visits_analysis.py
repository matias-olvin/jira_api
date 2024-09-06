"""
DAG ID: site_selection
"""
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


def register(dag, start):
    """Register tasks on the dag.

    Args:
        dag (airflow.models.DAG): DAG instance to register tasks on.
        start (airflow.models.TaskInstance): Task instance all tasks will
            registered downstream from.

    Returns:
        airflow.models.TaskInstance: The last task node in this section.
    """
    # Visits historical monthly visits
    query_monthly_visits = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="query_monthly_visits",
        project_id=Variable.get("compute_project_id"),
        configuration={
            "query": {
                "query": "{% include './bigquery/monthly_visits.sql' %}",
                "useLegacySql": "False",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id }}",
                "task_id": "{{ task.task_id.lower()[:63]}}",
            },
        },
        dag=dag,
        location="EU",
    )
    start >> query_monthly_visits

    return query_monthly_visits