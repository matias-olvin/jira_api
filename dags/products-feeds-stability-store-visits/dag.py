import glob
import os
from datetime import datetime
from importlib.machinery import SourceFileLoader

from airflow.exceptions import AirflowSkipException
from airflow.models import DAG, Variable
from airflow.models.dag import get_last_dagrun
from airflow.models.taskinstance import TaskInstance
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryValueCheckOperator,
)
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.session import provide_session
from common.operators.bigquery import OlvinBigQueryOperator
from common.utils import callbacks, dag_args, slack_users

path = f"{os.path.dirname(os.path.realpath(__file__))}"
DAG_ID = path.split("/")[-1]
steps_path = f"{path}/steps"

export_tasks = SourceFileLoader(
    "export_tasks", f"{steps_path}/export_tasks.py"
).load_module()
export_tasks_daily = SourceFileLoader(
    "export_tasks_daily", f"{steps_path}/export_tasks_daily.py"
).load_module()
daily_feeds_checks = SourceFileLoader(
    "daily_feeds_checks", f"{steps_path}/daily_feeds_checks.py"
).load_module()

env_args = dag_args.make_env_args(schedule_interval="0 6 1 * *")

default_args = dag_args.make_default_args(
    start_date=datetime(2022, 9, 16),
    depends_on_past=True,
    wait_for_downstream=True,
    retries=3,
    on_failure_callback=callbacks.task_fail_slack_alert(
        slack_users.MATIAS, channel=env_args["env"]
    ),
)
        
def branch_task_places_fn(**kwargs):

    execution_date = kwargs["execution_date"]
    if not isinstance(execution_date, datetime):
        execution_date = datetime.strptime(execution_date, "%Y-%m-%d")

    smc_end_date_prev = Variable.get("smc_end_date_prev")
    if not isinstance(smc_end_date_prev, datetime):
        smc_end_date_prev = datetime.strptime(smc_end_date_prev, "%Y-%m-%d")

    if execution_date.date() >= smc_end_date_prev.date():
        return "insert_store_visits_into_temp_increment"
    else:
        interval = (smc_end_date_prev.date() - execution_date.date()).days
        if interval > 24:
            return "insert_store_visits_into_temp_increment"
        else:
            return "set_version_places"

def set_version_fn(table_path: str, set_places_version: bool):
    """
    Extract current execution_date from table:
        storage-prod-olvin-com.monthly_update.monthly_update.

    Returns:
        execution_date (pushes to XCom).
    """
    from google.cloud import bigquery

    # create BigQuery Client object.
    client = bigquery.Client()
    query = f"""
        SELECT
        id
        FROM
        `{table_path}`
        WHERE
        start_date=(Select MAX(start_date) from `{table_path}`)
    """
    query_job = client.query(query)
    query_result = [row[0] for row in query_job.result()][0]

    def set_airflow_var(key, val):
        Variable.set(f"{key}", f"{val}")

    # Set airflow var
    if set_places_version:
        set_airflow_var("data_feed_places_version", f"{query_result}")
    else:
        set_airflow_var("data_feed_data_version", f"{query_result}")

    # push to XCom
    return str(query_result)


with DAG(
    DAG_ID,
    default_args=default_args,
    schedule_interval=env_args["schedule_interval"],
    params=dag_args.load_config(__file__),
    doc_md=dag_args.load_docs(__file__),
) as dag:

    @provide_session
    def exec_date_fn(execution_date, session=None, **kwargs):
        monthly_update_run_id = get_last_dagrun(
            dag_id=f"{dag.params['dag-products-postgres']}",
            session=session,
            include_externally_triggered=True,
        )

        return monthly_update_run_id.execution_date

    start = EmptyOperator(task_id="start")

    wait_for_monthly_update_completion = ExternalTaskSensor(
        task_id="wait_for_monthly_update_completion",
        external_dag_id=f"{dag.params['dag-products-postgres']}",
        external_task_id="start_sending_to_backend",
        depends_on_past=False,
        check_existence=True,
        execution_date_fn=exec_date_fn,
        mode="reschedule",
        poke_interval=60,
    )
    # Create task to run query_execution_date function.
    # Return statement in function will push result to XCom.
    set_version_places = PythonOperator(
        task_id="set_version_places",
        provide_context=True,
        python_callable=set_version_fn,
        op_kwargs={
            "table_path": "{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['Version_table'] }}",
            "set_places_version": True,
        },
    )

    set_version_monthly_update = PythonOperator(
        task_id="set_version_monthly_update",
        provide_context=True,
        python_callable=set_version_fn,
        op_kwargs={
            "table_path": "{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['Version_table'] }}",
            "set_places_version": False,
        },
    )

    start >> wait_for_monthly_update_completion

    branch_task_places = BranchPythonOperator(
        task_id="branch_task_places",
        provide_context=True,
        python_callable=branch_task_places_fn,
        dag=dag,
    )
    insert_store_visits_into_temp_backfill = BigQueryInsertJobOperator(
        task_id="insert_store_visits_into_temp_backfill",
        configuration={
            "query": {
                "query": "{% include './bigquery/store_visits_temp_backfill.sql' %}",
                "useLegacySql": "false",
            }
        },
        params=dag.params,
    )

    insert_store_visits_into_temp_increment = BigQueryInsertJobOperator(
        task_id="insert_store_visits_into_temp_increment",
        configuration={
            "query": {
                "query": "{% include './bigquery/store_visits_temp_increment.sql' %}",
                "useLegacySql": "false",
            }
        },
        params=dag.params,
    )

    create_snapshot_places = BigQueryInsertJobOperator(
        task_id="create_snapshot_places",
        configuration={
            "query": {
                "query": "{% include './bigquery/create_snapshot_places.sql' %}",
                "useLegacySql": "false",
            }
        },
        params=dag.params,
    )

    create_snapshot_monthly_update = BigQueryInsertJobOperator(
        task_id="create_snapshot_monthly_update",
        configuration={
            "query": {
                "query": "{% include './bigquery/create_snapshot_monthly_update.sql' %}",
                "useLegacySql": "false",
            }
        },
        params=dag.params,
    )

    # DATA QUALITY CHECKS
    data_quality_start = EmptyOperator(
        task_id="data_quality_checks_start",
        trigger_rule="none_failed_min_one_success",
        wait_for_downstream=False,
    )
    data_quality_end = EmptyOperator(
        task_id="data_quality_checks_end", trigger_rule="all_done"
    )

    (
        wait_for_monthly_update_completion
        >> set_version_monthly_update
        >> create_snapshot_monthly_update
    )
    (
        create_snapshot_monthly_update
        >> branch_task_places
        >> set_version_places
        >> create_snapshot_places
        >> insert_store_visits_into_temp_backfill
    )
    (
        create_snapshot_monthly_update
        >> branch_task_places
        >> insert_store_visits_into_temp_increment
    )

    [
        insert_store_visits_into_temp_increment,
        insert_store_visits_into_temp_backfill,
    ] >> data_quality_start

    # Looping through all the files in the data_quality folder and running the data quality checks.
    for file in glob.glob(
        f"{os.path.dirname(os.path.relpath(__file__))}/bigquery/data_quality/*.sql"
    ):
        # Getting the file name from the file path.
        file_name = str(file).split("/")[-1].replace(".sql", "")
        data_quality_task = BigQueryValueCheckOperator(
            task_id=f"{file_name}",
            sql=f"{{% include './bigquery/data_quality/{file_name}.sql' %}}",
            use_legacy_sql=False,
            depends_on_past=False,
            pass_value=0,
            retries=0,
            params=dag.params,
        )
        data_quality_start >> data_quality_task >> data_quality_end
    

    fix_multiple_ids = OlvinBigQueryOperator(
        task_id="fix_multiple_ids",
        query="{% include './bigquery/tweaks/fix_multiple_ids.sql' %}",
    )


    check_multiple_ids = OlvinBigQueryOperator(
        task_id="check_multiple_ids",
        query="{% include './bigquery/data_quality/check_multiple_ids/check_multiple_ids.sql' %}",
    )


    assert_volume = BigQueryInsertJobOperator(
        task_id="assert_volume",
        configuration={
            "query": {
                "query": "{% include './bigquery/assert_visits_volume.sql' %}",
                "useLegacySql": "false",
            }
        },
    )



    data_quality_end >> fix_multiple_ids >> check_multiple_ids >> assert_volume

    create_store_visits_daily_temp = OlvinBigQueryOperator(
        task_id="create_store_visits_daily_temp",
        query="{% include './bigquery/store_visits_daily/create_store_visits_daily_temp.sql' %}",
        billing_tier="med",
    )

    assert_volume >> create_store_visits_daily_temp

    daily_feeds_checks_end = daily_feeds_checks.register(start=create_store_visits_daily_temp, dag=dag)

    # Exporting the data to GCS.
    DATAPROC_CLUSTER_NAME = "pby-product-p-dpc-euwe1-stab-export-store-visits"
    DATAPROC_CLUSTER_DAILY_NAME = "pby-product-p-dpc-euwe1-stab-export-str-vsts-d"

    export_tasks_end = export_tasks.register(
        start=daily_feeds_checks_end,
        dag=dag,
        dataproc_name=DATAPROC_CLUSTER_NAME,
        dag_id=DAG_ID,
    )

    export_tasks_daily_end = export_tasks_daily.register(
        start=daily_feeds_checks_end,
        dag=dag,
        dataproc_name=DATAPROC_CLUSTER_DAILY_NAME,
        dag_id=DAG_ID,
    )

    end = EmptyOperator(task_id="end")

    [export_tasks_end, export_tasks_daily_end] >> end
