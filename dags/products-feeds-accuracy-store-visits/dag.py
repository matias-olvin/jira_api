import glob
import os
from datetime import datetime
from importlib.machinery import SourceFileLoader

from airflow.models import DAG, Variable
from airflow.models.dag import get_last_dagrun
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryValueCheckOperator,
)
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.session import provide_session
from common.operators.bigquery import OlvinBigQueryOperator
from common.utils import callbacks, dag_args, slack_users

path = f"{os.path.dirname(os.path.realpath(__file__))}"
DAG_ID = path.split("/")[-1]
steps_path = f"{path}/steps"

dpc_export_tables = SourceFileLoader(
    "dpc_export_tables", f"{steps_path}/dpc_export_tables.py"
).load_module()
dpc_export_tables_daily = SourceFileLoader(
    "dpc_export_tables_daily", f"{steps_path}/dpc_export_tables_daily.py"
).load_module()
daily_tables_checks = SourceFileLoader(
    "daily_tables_checks", f"{steps_path}/daily_tables_checks.py"
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


def set_version_fn(table_path: str):
    """
    Extract current execution_date from table:
        storage-prod-olvin-com.postgres_batch.Version.

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
    start = EmptyOperator(task_id="start")

    @provide_session
    def exec_date_fn(execution_date, session=None, **kwargs):
        monthly_update_run_id = get_last_dagrun(
            dag_id=f"{dag.params['dag-products-postgres']}",
            session=session,
            include_externally_triggered=True,
        )

        return monthly_update_run_id.execution_date

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

    end = EmptyOperator(task_id="end")

    start >> wait_for_monthly_update_completion

    set_version_monthly_update = PythonOperator(
        task_id="set_version_monthly_update",
        provide_context=True,
        python_callable=set_version_fn,
        op_kwargs={
            "table_path": "{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['Version_table'] }}",
        },
    )

    insert_store_visits_into_temp = BigQueryInsertJobOperator(
        task_id="insert_store_visits_into_temp",
        configuration={
            "query": {
                "query": "{% include './bigquery/store_visits_temp.sql' %}",
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

    (
        wait_for_monthly_update_completion
        >> set_version_monthly_update
        >> create_snapshot_places
        >> create_snapshot_monthly_update
    )
    create_snapshot_monthly_update >> insert_store_visits_into_temp

    # DATA QUALITY CHECKS
    data_quality_start = EmptyOperator(
        task_id="data_quality_checks_start", wait_for_downstream=False
    )
    data_quality_end = EmptyOperator(task_id="data_quality_checks_end")

    insert_store_visits_into_temp >> data_quality_start

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
        billing_tier="med",
    )

    check_multiple_ids = OlvinBigQueryOperator(
        task_id="check_multiple_ids",
        query="{% include './bigquery/data_quality/check_multiple_ids/check_multiple_ids.sql' %}",
        billing_tier="med",
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
        query="{% include './bigquery/daily_tables/create_store_visits_daily_temp.sql' %}",
        billing_tier="high",
    )

    assert_volume >> create_store_visits_daily_temp

    daily_tables_checks_end = daily_tables_checks.register(
        start=create_store_visits_daily_temp, dag=dag
    )

    DATAPROC_CLUSTER_NAME = "pby-product-p-dpc-euwe1-acc-export-store-visits"
    DATAPROC_CLUSTER_NAME_DAILY = "pby-product-p-dpc-euwe1-acc-expt-strs-vsts-daily"

    dpc_export_tables_end = dpc_export_tables.register(
        start=daily_tables_checks_end,
        dag=dag,
        dag_id=DAG_ID,
        dataproc_name=DATAPROC_CLUSTER_NAME,
    )

    dpc_export_tables_daily_end = dpc_export_tables_daily.register(
        start=daily_tables_checks_end,
        dag=dag,
        dag_id=DAG_ID,
        dataproc_name=DATAPROC_CLUSTER_NAME_DAILY,
    )

    [dpc_export_tables_end, dpc_export_tables_daily_end] >> end
