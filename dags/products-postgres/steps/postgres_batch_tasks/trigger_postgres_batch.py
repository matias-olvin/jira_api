from airflow.models import DAG, TaskInstance, Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryValueCheckOperator
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
    execution_date = Variable.get("monthly_update")
    postgres_batch_dag_id = "postgres_batch"

    move_to_postgres_batch_end = EmptyOperator(task_id="move_to_postgres_batch_end")

    move_to_postgres_batch_copy_queries = list()

    tables_to_send = dag.params["postgres_db_tables_in_bq"] + dag.params["data_feeds_tables"]
    avoid_copy_from_postgres_to_postgres_batch_list = dag.params["avoid_copy_from_postgres_to_postgres_batch"]

    tables_to_send_filtered = list(filter(lambda x: x not in avoid_copy_from_postgres_to_postgres_batch_list, tables_to_send))

    for table in tables_to_send_filtered:

        source = f"{{{{ var.value.env_project }}}}.{{{{ params['postgres_dataset'] }}}}.{table}"
        destination = f"{{{{ var.value.env_project }}}}.{{{{ params['postgres_batch_dataset'] }}}}.{table}"

        _query = copy_table(source=source, destination=destination)

        move_to_postgres_batch_copy_queries.append(_query)

    move_to_postgres_batch_task = OlvinBigQueryOperator.partial(
        task_id="move_tables_to_postgres_batch"
    ).expand(query=move_to_postgres_batch_copy_queries)

    # Triggering the send_to_postgres_pipeline DAG.
    trigger_postgres_batch = BashOperator(
        task_id="trigger_postgres_batch",
        bash_command="gcloud composer environments run {{ params['sns_composer'] }} "
        "--project {{ params['sns_project'] }} "
        "--location {{ params['sns_composer_location'] }} "
        "--impersonate-service-account {{ params['cross_project_service_account'] }} "
        f'dags trigger -- {postgres_batch_dag_id} -e "{execution_date}" '
        "|| true ",  # this line is needed due to gcloud bug.
    )

    check_postgres_batch_status = BigQueryValueCheckOperator(
        gcp_conn_id="google_cloud_olvin_default",
        task_id="check_postgres_batch_status",
        sql="{% include './bigquery/check_pipeline_complete.sql' %}",
        pass_value=True,
        use_legacy_sql=False,
        params={"pipeline": f"{postgres_batch_dag_id}"},
        retries=60,
        retry_delay=60 * 2,
    )

    (
        start
        >> move_to_postgres_batch_task
        >> move_to_postgres_batch_end
        >> trigger_postgres_batch
        >> check_postgres_batch_status
    )

    return check_postgres_batch_status
