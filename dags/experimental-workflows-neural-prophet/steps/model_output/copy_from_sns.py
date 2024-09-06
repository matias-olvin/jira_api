"""
DAG ID: visits_estimation_model_development
"""
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

    copy_from_sns = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="copy_from_sns",
        configuration={
            "query": {
                "query": "{% include './bigquery/model_output/copy_from_sns.sql' %}",
                "useLegacySql": "False",
            }
        },
        dag=dag,
    )

    start >> copy_from_sns

    return copy_from_sns
