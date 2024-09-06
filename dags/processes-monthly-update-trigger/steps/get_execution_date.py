from airflow.models import DAG, TaskInstance, Variable
from airflow.operators.python import PythonOperator
from common.operators.bigquery import OlvinBigQueryOperator
from google.cloud import bigquery


def register(start: TaskInstance, dag: DAG) -> TaskInstance:
    """
    Register tasks on the dag.

    Args:
        dag (airflow.models.DAG): DAG instance to register tasks on.
        start (airflow.models.TaskInstance): Task instance all tasks will
            registered downstream from.

    Returns:
        airflow.models.TaskInstance: The last task node in this section.
    """

    def query_execution_date(table_path: str):
        """
        Extract current execution_date from table:
            storage-prod-olvin-com.monthly_update.monthly_update.

        Returns:
            execution_date (pushes to XCom).
        """

        # create BigQuery Client object.
        client = bigquery.Client()
        query = f"""
            SELECT
                MIN(execution_date)
            FROM
                `{table_path}`
            WHERE
                NOT completed
        """
        query_job = client.query(query)
        query_result = [row[0] for row in query_job.result()][0]

        def set_airflow_var(key, val):
            Variable.set(f"{key}", f"{val}")

        # Set airflow var
        set_airflow_var("monthly_update", f"{query_result}")

        # push to XCom
        return str(query_result)

    # For idempotency - remove any execution_date which is not completed from table:
    remove_uncompleted_updates = OlvinBigQueryOperator(
        task_id="remove_uncompleted_updates",
        query="{% include './bigquery/remove_uncompleted_updates.sql' %}",
    )

    # Insert current update execution date into table:
    insert_current_update = OlvinBigQueryOperator(
        task_id="insert_current_update",
        query="{% include './bigquery/insert_current_update.sql' %}",
    )

    # Create task to run query_execution_date function.
    # Return statement in function will push result to XCom.
    push_execution_date = PythonOperator(
        task_id="push_execution_date",
        provide_context=True,
        python_callable=query_execution_date,
        op_kwargs={
            "table_path": "{{ var.value.env_project }}.{{ params['test_compilation_dataset'] }}.{{ params['monthly_update_table'] }}"
        },
    )

    start >> remove_uncompleted_updates >> insert_current_update >> push_execution_date

    return push_execution_date
