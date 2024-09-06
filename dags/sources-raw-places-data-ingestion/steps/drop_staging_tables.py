"""
DAG ID: dynamic_places
"""
from airflow.operators.dummy_operator import DummyOperator
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
    drop_staging_tables_start = DummyOperator(
        task_id="drop_staging_tables_start",
        dag=dag,
    )
    start >> drop_staging_tables_start

    drop_staging_tables_end = DummyOperator(
        task_id="drop_staging_tables_end",
        dag=dag,
    )

    staging_tables = ["sg_raw", "sg_places", "lineage", "sg_brands"]

    for staging_table in staging_tables:
        drop_staging_table = BigQueryInsertJobOperator(
            task_id=f"drop_{staging_table}_staging",
            configuration={
                "query": {
                    "query": "{% include './bigquery/clean_up/drop_staging_table.sql' %}",
                    "useLegacySql": "False",
                },
                "labels": {
                    "pipeline": "{{ dag.dag_id }}",
                    "task_id": "{{ task.task_id.lower()[:63] }}",
                },
            },
            params={"staging_table": staging_table},
            dag=dag,
            location="EU",
        )
        drop_staging_tables_start >> drop_staging_table >> drop_staging_tables_end

    return drop_staging_tables_end
