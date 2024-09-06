"""
DAG ID: dynamic_places
"""
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


def register(dag, start):
    """
    Register tasks on the dag

    Args:
        dag (airflow.models.DAG): DAG instance to register tasks on.
        start (airflow.models.TaskInstance): Task instance all tasks will
        be registered downstream from.

    Returns:
        airflow.models.TaskInstance: The last task node in this section.
    """
    create_places_table_part_1 = BigQueryInsertJobOperator(
        gcp_conn_id="google_cloud_olvin_default",
        task_id="create_places_table_part_1",
        configuration={
            "query": {
                "query": "{% include './bigquery/places/create_sg_places_1.sql' %}",
                "useLegacySql": "False",
                "create_session": "True",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )
    start >> create_places_table_part_1

    return create_places_table_part_1
