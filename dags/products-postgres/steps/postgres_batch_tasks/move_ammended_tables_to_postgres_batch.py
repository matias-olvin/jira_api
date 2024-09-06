from airflow.models import DAG, TaskInstance
from airflow.operators.empty import EmptyOperator
from common.operators.bigquery import OlvinBigQueryOperator
from common.utils.queries import copy_table


def register(start: TaskInstance, dag: DAG) -> TaskInstance:
    """
    Register tasks on the dag

    Args:
        dag (airflow.models.DAG): DAG instance to register tasks on.
        start (airflow.models.TaskInstance): Task instance all tasks will
        be registered downstream from.

    Returns:
        airflow.models.TaskInstance: The last task node in this section.
    """

    def ammend_tables_query(source: str, destination: str) -> str:
        """
        Copy source table to the destination table in BigQuery. The destination table will be partitioned by local_date and clustered by fk_sgplaces

        Args:
            source (str): Source table (`project.dataset.table`) to copy from.
            destination (str): Destination table (`project.dataset.table`) to write to.

        Returns:
            a string that represents a BigQuery SQL query.
        """
        query_string = f"""
        create or replace table
            `{destination}`
        partition by local_date
        cluster by fk_sgplaces
        as (
            select *
            from
                `{source}`
        );
        """

        return query_string

    move_ammended_tables_to_postgres_batch_queries = list()

    for table in dag.params["visits_postgres_tables"]:

        destination = f"{{{{ var.value.env_project }}}}.{{{{ params['postgres_batch_dataset'] }}}}.{table}"
        source = f"{{{{ var.value.sns_project }}}}.{{{{ params['accessible_by_olvin_dataset'] }}}}.postgres_sns-{table}"

        if table in ["SGPlaceRanking", "SGPlaceRaw"]:
            _query = copy_table(source=source, destination=destination)
        else:
            _query = ammend_tables_query(source=source, destination=destination)

        move_ammended_tables_to_postgres_batch_queries.append(_query)

    move_to_postgres_batch_task = OlvinBigQueryOperator.partial(
        task_id="move_ammended_tables_to_postgres_batch",
        billing_tier="med",
    ).expand(query=move_ammended_tables_to_postgres_batch_queries)

    move_ammended_to_postgres_batch_end = EmptyOperator(
        task_id="move_ammended_tables_to_postgres_batch_end"
    )

    (start >> move_to_postgres_batch_task >> move_ammended_to_postgres_batch_end)

    return move_ammended_to_postgres_batch_end
