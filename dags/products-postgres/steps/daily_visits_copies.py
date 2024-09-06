from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy_operator import DummyOperator
from common.operators.bigquery import OlvinBigQueryOperator, SNSBigQueryOperator


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
    with TaskGroup(group_id="daily_visits_copies") as group:
        copy_tasks_for_daily_visits_start = DummyOperator(
            task_id="copy_tasks_for_daily_visits_start"
        )
        start >> copy_tasks_for_daily_visits_start

        copy_tasks_for_daily_visits_end = DummyOperator(
            task_id="copy_tasks_for_daily_visits_end"
        )

        copy_to_postgres_rt = OlvinBigQueryOperator(
            task_id="copy_to_postgres_rt",
            query='{% include "./bigquery/daily_visits_copies/copy_to_postgres_rt.sql" %}',
        )
        copy_tasks_for_daily_visits_start >> copy_to_postgres_rt

        copy_to_accessible_by_sns = OlvinBigQueryOperator(
            task_id="copy_to_accessible_by_sns",
            query='{% include "./bigquery/daily_visits_copies/copy_to_accessible_by_sns.sql" %}',
        )

        copy_to_accessible_by_olvin = SNSBigQueryOperator(
            task_id="copy_to_accessible_by_olvin",
            query='{% include "./bigquery/daily_visits_copies/copy_to_accessible_by_olvin.sql" %}',
        )

        copy_to_postgres_rt >> copy_to_accessible_by_sns >> copy_to_accessible_by_olvin >> copy_tasks_for_daily_visits_end

    return group