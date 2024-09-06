"""
DAG ID: monthly_update_trigger
"""
# Importing the necessary modules from the Airflow library.
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryValueCheckOperator,
)


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
    execution_date = Variable.get("monthly_update")
    trigger_dag_id = "almanac_visits_static_accuracy"

    move_visits_tables_end = DummyOperator(task_id="move_visits_tables_end")
    for table in dag.params["visits_postgres_tables"]:
        query = (
            f"""
        create or replace table
            `{Variable.get('env_project')}.{dag.params['accessible_by_sns_dataset']}.{dag.params['postgres_dataset']}-{table}`
        copy
            `{Variable.get('env_project')}.{dag.params['postgres_dataset']}.{table}`;
        """
            if table in {"SGPlaceRanking", "SGPlaceRaw"}
            else f"""
        create or replace table
            `{Variable.get('env_project')}.{dag.params['accessible_by_sns_dataset']}.{dag.params['postgres_dataset']}-{table}`
        partition by local_date
        cluster by fk_sgplaces
        as (
            select *
            from
                `{Variable.get('env_project')}.{dag.params['postgres_dataset']}.{table}`
        );
        """
        )
        move_visits_table_task = BigQueryInsertJobOperator(
            gcp_conn_id="google_cloud_olvin_default",
            task_id=f"move_{table}_to_accessibly_by_sns",
            configuration={"query": {"query": query, "useLegacySql": "false"}},
        )
        label_visits_table_task = BigQueryInsertJobOperator(
            gcp_conn_id="google_cloud_olvin_default",
            task_id=f"label_{table}_in_accessibly_by_sns",
            configuration={
                "query": {
                    "query": "ALTER TABLE "
                    f" `{Variable.get('env_project')}.{dag.params['accessible_by_sns_dataset']}.{dag.params['postgres_dataset']}-{table}` "
                    "SET OPTIONS ("
                    "  labels = [('dataset', '{{ params['postgres_dataset'] }}')]"
                    ");",
                    "useLegacySql": "false",
                }
            },
        )
        (
            start
            >> move_visits_table_task
            >> label_visits_table_task
            >> move_visits_tables_end
        )

    move_openings_adjustment_table = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="move_openings_adjustment_table",
        configuration={
            "query": {
                "query": "create or replace table "
                f"    `{Variable.get('env_project')}.{dag.params['accessible_by_sns_dataset']}.{dag.params['openings_adjustment_table']}` "
                "copy  "
                f"    `{Variable.get('env_project')}.{dag.params['sg_places_dataset']}.{dag.params['openings_adjustment_table']}`;",
                "useLegacySql": "false",
            }
        },
    )
    start >> move_openings_adjustment_table >> move_visits_tables_end

    return move_visits_tables_end
