from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


def register(dag, start):
    """
    Register tasks on the dag.

    Args:
        dag (airflow.models.DAG): DAG instance to register tasks on.
        start (airflow.models.TaskInstance): Task instance all tasks will
            registered downstream from.

    Returns:
        airflow.models.TaskInstance: The last task node in this section.
    """
    update_postgres_batch_derived_start = DummyOperator(
        task_id="update_postgres_batch_derived_start"
    )
    start >> update_postgres_batch_derived_start

    update_postgres_batch_derived_end = DummyOperator(
        task_id="update_postgres_batch_derived_end"
    )

    update_final_SGPlacePatternVisitsRaw_table = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="update_final_SGPlacePatternVisitsRaw_table",
        configuration={
            "query": {
                "query": "{% include './bigquery/update_postgres_batch_derived_tables/SGPlacePatternVisitsRaw.sql' %}",
                "useLegacySql": "False",
            }
        },
        dag=dag,
        location="EU",
    )


    update_postgres_batch_derived_start >> update_final_SGPlacePatternVisitsRaw_table
    update_final_SGPlacePatternVisitsRaw_table >> update_postgres_batch_derived_end


    return update_postgres_batch_derived_end
