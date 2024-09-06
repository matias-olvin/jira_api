from airflow.operators.empty import EmptyOperator
from common.operators.bigquery import OlvinBigQueryOperator


def register(dag, start, postgres_dataset):
    """
    Register tasks on the dag.

    Args:
        dag (airflow.models.DAG): DAG instance to register tasks on.
        start (airflow.models.TaskInstance): Task instance all tasks will
            registered downstream from.

    Returns:
        airflow.models.TaskInstance: The last task node in this section.
    """
    update_centers_table_start = EmptyOperator(
        task_id=f"update_centers_table_start_{postgres_dataset}"
    )

    update_SGCenterRaw_table = OlvinBigQueryOperator(
        task_id=f"update_SGCenterRaw_table_{postgres_dataset}",
        query='{% with postgres_dataset="'
        f"{postgres_dataset}"
        '"%}{% include "./bigquery/update_malls_tables/update_centers_table.sql" %}{% endwith %}',
    )

    update_centers_table_end = EmptyOperator(
        task_id=f"update_centers_table_end_{postgres_dataset}"
    )

    (
        start
        >> update_centers_table_start
        >> update_SGCenterRaw_table
        >> update_centers_table_end
    )

    return update_centers_table_end
