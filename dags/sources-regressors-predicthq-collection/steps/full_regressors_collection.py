"""
DAG ID: predicthq_events_collection
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

    full_regressors_collection = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="full_regressors_collection",
        configuration={
            "query": {
                "query": "{% include './bigquery/full_regressors_collection.sql' %}",
                "useLegacySql": "False",
                "destinationTable": {
                    "projectId": "{{ var.value.env_project }}",
                    "datasetId": "{{ params['regressors_dataset'] }}",
                    "tableId": "{{ params['regressors_table']}}",
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_IF_NEEDED",
            },
            "labels": {
                "dag_id": "{{ dag.dag_id }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )

    start >> full_regressors_collection

    return full_regressors_collection
