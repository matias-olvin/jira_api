import glob
import os
from datetime import datetime

from airflow.models import DAG, Variable
from airflow.exceptions import AirflowSkipException
from airflow.models.taskinstance import TaskInstance
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryValueCheckOperator,
)
from airflow.utils.task_group import TaskGroup
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator,
)
from common.operators.bigquery import OlvinBigQueryOperator
from airflow.operators.python_operator import BranchPythonOperator
from common.utils import dag_args, callbacks, slack_users
from airflow.utils.session import provide_session
from airflow.models.dag import get_last_dagrun
from airflow.sensors.external_task import ExternalTaskSensor

folder_path = f"{os.path.dirname(os.path.realpath(__file__))}"
DAG_ID = folder_path.split("/")[-1]

env_args = dag_args.make_env_args(
    schedule_interval="0 6 1 * *"
)
default_args=dag_args.make_default_args(
    start_date=datetime(2022, 9, 16),
    depends_on_past=True,
    wait_for_downstream=True,
    retries=3,
    on_failure_callback=callbacks.task_fail_slack_alert(
        slack_users.MATIAS, channel=env_args["env"]
    ),
)

def branch_task_places_fn(**kwargs):
    if str(kwargs['execution_date'])>=Variable.get("smc_end_date_prev"):
        return 'insert_store_visits_trend_into_temp_increment'
    else:
        smc_end_date = Variable.get("smc_end_date_prev")
        exec_date = str(kwargs['execution_date'])
        interval=(datetime.strptime(smc_end_date, '%Y-%m-%d').date()-datetime.strptime(exec_date.split('T')[0], '%Y-%m-%d').date()).days
        if interval>24:
            return 'insert_store_visits_trend_into_temp_increment'
        else:
            return 'set_version_places' 
    
def branch_task_insert_fn(**kwargs):
    if str(kwargs['execution_date'])>=Variable.get("smc_end_date_prev"):
        return 'data-feed-export-increment.insert_store_visits_trend'
    else:
        smc_end_date = Variable.get("smc_end_date_prev")
        exec_date = str(kwargs['execution_date'])
        interval=(datetime.strptime(smc_end_date, '%Y-%m-%d').date()-datetime.strptime(exec_date.split('T')[0], '%Y-%m-%d').date()).days
        if interval>24:
            return 'data-feed-export-increment.insert_store_visits_trend'
        else:
            return 'data-feed-export-backfill.insert_store_visits_trend'
    
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
            "set_places_version":True,
        },
    )

    set_version_monthly_update = PythonOperator(
        task_id="set_version_monthly_update",
        provide_context=True,
        python_callable=set_version_fn,
        op_kwargs={
            "table_path": "{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['Version_table'] }}",
            "set_places_version":False,
        },
    )
    
    start = DummyOperator(task_id="start")
    start >> wait_for_monthly_update_completion
    end = DummyOperator(task_id="end")

    branch_task_places = BranchPythonOperator(
    task_id='branch_task_places',
    provide_context=True,
    python_callable=branch_task_places_fn,
    dag=dag
    )
    insert_store_visits_trend_into_temp_backfill = BigQueryInsertJobOperator(
        task_id="insert_store_visits_trend_into_temp_backfill",
        configuration={
            "query": {
                "query": "{% include './bigquery/store_visits_trend_temp_backfill.sql' %}",
                "useLegacySql": "false",
            }
        },
        params=dag.params,
    )

    insert_store_visits_trend_into_temp_increment = BigQueryInsertJobOperator(
        task_id="insert_store_visits_trend_into_temp_increment",
        configuration={
            "query": {
                "query": "{% include './bigquery/store_visits_trend_temp_increment.sql' %}",
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
    data_quality_start = DummyOperator(task_id="data_quality_checks_start", trigger_rule="none_failed_min_one_success", wait_for_downstream=False)
    data_quality_end = DummyOperator(
        task_id="data_quality_checks_end", trigger_rule="all_done"
    )
    wait_for_monthly_update_completion >> set_version_monthly_update >> create_snapshot_monthly_update
    create_snapshot_monthly_update >> branch_task_places >> set_version_places >> create_snapshot_places >> insert_store_visits_trend_into_temp_backfill
    create_snapshot_monthly_update >> branch_task_places >> insert_store_visits_trend_into_temp_increment

    [insert_store_visits_trend_into_temp_increment, insert_store_visits_trend_into_temp_backfill] >> data_quality_start

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

    # DATA TWEAKS
    data_tweaks_start = DummyOperator(task_id="data_tweaks_start")
    data_tweaks_end = DummyOperator(
        task_id="data_tweaks_end", trigger_rule="all_done"
    )

    data_quality_end >> data_tweaks_start

    def check_status(check_task_id: str, **context) -> None:
        """
        If the task with the id check_task_id is successful, then skip the task that calls this function

        :param check_task_id: The task id of the task to check the status of
        """
        ti = TaskInstance(
            context["dag"].get_task(check_task_id), context["execution_date"]
        )
        state = ti.current_state()
        if state == "success":
            raise AirflowSkipException()

    # Looping through all the files in the data_quality folder and running the data quality checks.
    for file in glob.glob(
        f"{os.path.dirname(os.path.relpath(__file__))}/bigquery/tweaks/*.sql"
    ):
        # Getting the file name from the file path.
        file_name = str(file).split("/")[-1].replace(".sql", "")
        # The `check_task_id` is the task id of the data quality check that the data tweak is fixing.
        check_task_id = file_name.replace("fix_", "check_")

        check_task_status = PythonOperator(
            task_id=f"{check_task_id}_test_status",
            python_callable=check_status,
            op_kwargs={
                "check_task_id": check_task_id,
            },
        )
        data_tweaks_start >> check_task_status

        data_tweak_task = BigQueryInsertJobOperator(
            task_id=f"{file_name}",
            configuration={
                "query": {
                    "query": f"{{% include './bigquery/tweaks/{file_name}.sql' %}}",
                    "useLegacySql": "false",
                }
            },
        )
        check_task_status >> data_tweak_task >> data_tweaks_end

    # DATA QUALITY CHECKS
    second_data_quality_start = DummyOperator(task_id="second_data_quality_checks_start")
    second_data_quality_end = DummyOperator(task_id="second_data_quality_checks_end")

    data_tweaks_end >> second_data_quality_start

    # Looping through all the files in the data_quality folder and running the data quality checks.
    for file in glob.glob(
        f"{os.path.dirname(os.path.relpath(__file__))}/bigquery/data_quality/*.sql"
    ):
        # Getting the file name from the file path.
        file_name = str(file).split("/")[-1].replace(".sql", "")
        second_data_quality_task = BigQueryValueCheckOperator(
            task_id=f"second_{file_name}",
            sql=f"{{% include './bigquery/data_quality/{file_name}.sql' %}}",
            use_legacy_sql=False,
            pass_value=0,
            retries=0,
            params=dag.params,
        )
        second_data_quality_start >> second_data_quality_task >> second_data_quality_end
    assert_volume = BigQueryInsertJobOperator(
        task_id="assert_volume",
        configuration={
            "query": {
                "query": "{% include './bigquery/assert_visits_volume.sql' %}",
                "useLegacySql": "false",
            }
        },
    )
    second_data_quality_end >> assert_volume

    branch_task_insert = BranchPythonOperator(
    task_id='branch_task_insert',
    provide_context=True,
    python_callable=branch_task_insert_fn,
    dag=dag
    )

    # Exporting the data to GCS.
    DATAPROC_CLUSTER_NAME="pby-product-p-dpc-euwe1-stab-export-store-visits-t"
    PARTITION_COLUMN="month_starting"
    BACKFILL_START=f"{dag.params['backfill_start_date']}"

    create_export_to_gcs_cluster = DataprocCreateClusterOperator(
        task_id="create_export_to_gcs_cluster",
        project_id=Variable.get("env_project"),
        cluster_config={
            "worker_config": {
                "num_instances": 2, 
                "machine_type_uri":"n1-highmem-16",
                "disk_config": {"boot_disk_type": "pd-ssd", "boot_disk_size_gb": 1024, "local_ssd_interface": "nvme", "num_local_ssds": 1},
            },
            "initialization_actions": [
                {
                    "executable_file": (
                        "gs://goog-dataproc-initialization-actions-"
                        "{{ params['dataproc_region'] }}/connectors/connectors.sh"
                    )
                }
            ],
            "gce_cluster_config": {
                "metadata": {
                    "bigquery-connector-version": "1.2.0",
                    "spark-bigquery-connector-version": "0.35.0",
                },
            },
            "lifecycle_config": {
                "auto_delete_ttl": {"seconds":7200},
                "idle_delete_ttl": {"seconds":3600},
            },
            "software_config": {
                "image_version": "2.0-ubuntu18",
                "properties": {
                    "spark:spark.sql.shuffle.partitions": "96", # num vCPUS available * 3 is recommended
                    "spark:spark.sql.adaptive.enabled": "true",
                    "spark:spark.dynamicAllocation.enabled": "true",
                    "dataproc:efm.spark.shuffle": "primary-worker",
                    "spark:spark.dataproc.enhanced.optimizer.enabled": "true",
                    "spark:spark.dataproc.enhanced.execution.enabled": "true",
                    "hdfs:dfs.replication": "1"
                },
            },
            "endpoint_config": {
                "enable_http_port_access": True,
            },
        },
        region=dag.params["dataproc_region"],
        cluster_name=DATAPROC_CLUSTER_NAME,
        dag=dag,
    )
    
    with TaskGroup(group_id="data-feed-export-backfill") as feed_export_backfill:
        insert_store_visits = BigQueryInsertJobOperator(
        task_id="insert_store_visits_trend",
        depends_on_past=False,
        configuration={
            "query": {
                "query": "{% include './bigquery/store_visits_trend_backfill.sql' %}",
                "useLegacySql": "false",
            }
        },
        params=dag.params,
    )
        
        clear_gcs = GCSDeleteObjectsOperator(
        task_id="clear_gcs",
        depends_on_past=False,
        bucket_name="{{ params['raw_feed_bucket'] }}",
        prefix="{{ params['store_visits_trend_table'].replace('_','-') }}/",
    )
        
        submit_export_to_gcs_job = DataprocSubmitJobOperator(
        task_id="submit_export_to_gcs_job",
        depends_on_past=False,
        job={
            "reference": {"project_id": "{{ var.value.env_project }}"},
            "placement": {"cluster_name": DATAPROC_CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": f"{{{{ var.value.gcs_dags_folder }}}}/{DAG_ID}/pyspark/runner.py",
                "python_file_uris": [f"{{{{ var.value.gcs_dags_folder }}}}/{DAG_ID}/pyspark/args.py"],
                "args": [
                    f"--project={{{{ var.value.env_project }}}}",
                    f"--input_table={{{{ params['public_feeds_dataset'] }}}}.{{{{ params['store_visits_trend_table'] }}}}",
                    f"--output_folder=gs://{{{{ params['raw_feed_bucket'] }}}}/{{{{ params['store_visits_trend_table'].replace('_','-') }}}}",
                    f"--partition_column={PARTITION_COLUMN}",
                    f"--num_files_per_partition=2",
                    f"--append_mode=overwrite",
                    f"--date_start={BACKFILL_START}",
                    f"--date_end={{{{ execution_date.add(months=1).strftime('%Y-%m-01') }}}}"
                ],
            },
        },
        region=dag.params["dataproc_region"],
        project_id=Variable.get("env_project"),
        dag=dag,
    )
        
        insert_store_visits >> clear_gcs >> submit_export_to_gcs_job
        

    with TaskGroup(group_id="data-feed-export-increment") as feed_export_increment:
        insert_store_visits = BigQueryInsertJobOperator(
        task_id="insert_store_visits_trend",
        depends_on_past=False,
        configuration={
            "query": {
                "query": "{% include './bigquery/store_visits_trend_increment.sql' %}",
                "useLegacySql": "false",
            }
        },
        params=dag.params,
    )
        
        clear_gcs = GCSDeleteObjectsOperator(
        task_id="clear_gcs",
        depends_on_past=False,
        bucket_name="{{ params['raw_feed_bucket'] }}",
        prefix=f"{{{{ params['store_visits_trend_table'].replace('_','-') }}}}/{PARTITION_COLUMN}={{{{ execution_date.strftime('%Y-%m') }}}}",
    )
        
        submit_export_to_gcs_job = DataprocSubmitJobOperator(
        task_id="submit_export_to_gcs_job",
        depends_on_past=False,
        job={
            "reference": {"project_id": "{{ var.value.env_project }}"},
            "placement": {"cluster_name": DATAPROC_CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": f"{{{{ var.value.gcs_dags_folder }}}}/{DAG_ID}/pyspark/runner.py",
                "python_file_uris": [f"{{{{ var.value.gcs_dags_folder }}}}/{DAG_ID}/pyspark/args.py"],
                "args": [
                    f"--project={{{{ var.value.env_project }}}}",
                    f"--input_table={{{{ params['public_feeds_dataset'] }}}}.{{{{ params['store_visits_trend_table'] }}}}",
                    f"--output_folder=gs://{{{{ params['raw_feed_bucket'] }}}}/{{{{ params['store_visits_trend_table'].replace('_','-') }}}}",
                    f"--partition_column={PARTITION_COLUMN}",
                    f"--num_files_per_partition=2",
                    f"--append_mode=append",
                    f"--date_start={{{{ execution_date.strftime('%Y-%m-01') }}}}",
                    f"--date_end={{{{ execution_date.add(months=1).strftime('%Y-%m-01') }}}}"
                ],
            },
        },
        region=dag.params["dataproc_region"],
        project_id=Variable.get("env_project"),
        dag=dag,
    )
        
        insert_store_visits >> clear_gcs >> submit_export_to_gcs_job


    with TaskGroup(group_id="data-feed-export-finance-backfill") as feed_export_finance_backfill:
        insert_store_visits = BigQueryInsertJobOperator(
        task_id="insert_store_visits_trend",
        depends_on_past=False,
        configuration={
            "query": {
                "query": "{% include './bigquery/store_visits_trend_finance.sql' %}",
                "useLegacySql": "false",
            }
        },
        params=dag.params,
    )
        
        clear_gcs = GCSDeleteObjectsOperator(
        task_id="clear_gcs",
        depends_on_past=False,
        bucket_name="{{ params['raw_feed_finance_bucket'] }}",
        prefix="{{ params['store_visits_trend_table'].replace('_','-') }}/",
    )
        
        submit_export_to_gcs_job = DataprocSubmitJobOperator(
        task_id="submit_export_to_gcs_job",
        depends_on_past=False,
        job={
            "reference": {"project_id": "{{ var.value.env_project }}"},
            "placement": {"cluster_name": DATAPROC_CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": f"{{{{ var.value.gcs_dags_folder }}}}/{DAG_ID}/pyspark/runner.py",
                "python_file_uris": [f"{{{{ var.value.gcs_dags_folder }}}}/{DAG_ID}/pyspark/args.py"],
                "args": [
                    f"--project={{{{ var.value.env_project }}}}",
                    f"--input_table={{{{ params['public_feeds_finance_dataset'] }}}}.{{{{ params['store_visits_trend_table'] }}}}",
                    f"--output_folder=gs://{{{{ params['raw_feed_finance_bucket'] }}}}/{{{{ params['store_visits_trend_table'].replace('_','-') }}}}",
                    f"--partition_column={PARTITION_COLUMN}",
                    f"--num_files_per_partition=2",
                    f"--append_mode=overwrite",
                    f"--date_start={BACKFILL_START}",
                    f"--date_end={{{{ execution_date.add(months=1).strftime('%Y-%m-01') }}}}"
                ],
            },
        },
        region=dag.params["dataproc_region"],
        project_id=Variable.get("env_project"),
        dag=dag,
    )
        
        insert_store_visits >> clear_gcs >> submit_export_to_gcs_job

    with TaskGroup(group_id="data-feed-export-finance-increment") as feed_export_finance_increment:
        insert_store_visits = BigQueryInsertJobOperator(
        task_id="insert_store_visits_trend",
        depends_on_past=False,
        configuration={
            "query": {
                "query": "{% include './bigquery/store_visits_trend_finance.sql' %}",
                "useLegacySql": "false",
            }
        },
        params=dag.params,
    )
        
        clear_gcs = GCSDeleteObjectsOperator(
        task_id="clear_gcs",
        depends_on_past=False,
        bucket_name="{{ params['raw_feed_finance_bucket'] }}",
        prefix=f"{{{{ params['store_visits_trend_table'].replace('_','-') }}}}/{PARTITION_COLUMN}={{{{ execution_date.strftime('%Y-%m') }}}}",
    )
        
        submit_export_to_gcs_job = DataprocSubmitJobOperator(
        task_id="submit_export_to_gcs_job",
        depends_on_past=False,
        job={
            "reference": {"project_id": "{{ var.value.env_project }}"},
            "placement": {"cluster_name": DATAPROC_CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": f"{{{{ var.value.gcs_dags_folder }}}}/{DAG_ID}/pyspark/runner.py",
                "python_file_uris": [f"{{{{ var.value.gcs_dags_folder }}}}/{DAG_ID}/pyspark/args.py"],
                "args": [
                    f"--project={{{{ var.value.env_project }}}}",
                    f"--input_table={{{{ params['public_feeds_finance_dataset'] }}}}.{{{{ params['store_visits_trend_table'] }}}}",
                    f"--output_folder=gs://{{{{ params['raw_feed_finance_bucket'] }}}}/{{{{ params['store_visits_trend_table'].replace('_','-') }}}}",
                    f"--partition_column={PARTITION_COLUMN}",
                    f"--num_files_per_partition=2",
                    f"--append_mode=append",
                    f"--date_start={{{{ execution_date.strftime('%Y-%m-01') }}}}",
                    f"--date_end={{{{ execution_date.add(months=1).strftime('%Y-%m-01') }}}}"
                ],
            },
        },
        region=dag.params["dataproc_region"],
        project_id=Variable.get("env_project"),
        dag=dag,
    )
        
        insert_store_visits >> clear_gcs >> submit_export_to_gcs_job

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=Variable.get("env_project"),
        cluster_name=DATAPROC_CLUSTER_NAME,
        region=dag.params["dataproc_region"],
        dag=dag,
        trigger_rule="none_failed_min_one_success",
    )

    # Exporting the data to GCS for versioning
    clear_store_visits_trend_for_versioning_backfill = GCSDeleteObjectsOperator(
            task_id="clear_store_visits_trend_for_versioning_backfill",
            bucket_name="{{ params['versioning_bucket'] }}",
            prefix="type-2/{{ params['store_visits_trend_table'] }}/{{ next_ds.replace('-', '/') }}/",
    )

    export_store_visits_trend_for_versioning_backfill = OlvinBigQueryOperator(
        task_id="export_store_visits_trend_for_versioning_backfill",
        query="{% include './bigquery/export_store_visits_trend_for_versioning_backfill.sql' %}",
        billing_tier="high",
    )

    clear_store_visits_trend_for_versioning_increment = GCSDeleteObjectsOperator(
            task_id="clear_store_visits_trend_for_versioning_increment",
            bucket_name="{{ params['versioning_bucket'] }}",
            prefix="type-2/{{ params['store_visits_trend_table'] }}/{{ next_ds.replace('-', '/') }}/",
    )

    export_store_visits_trend_for_versioning_increment = OlvinBigQueryOperator(
        task_id="export_store_visits_trend_for_versioning_increment",
        query="{% include './bigquery/export_store_visits_trend_for_versioning_increment.sql' %}",
        billing_tier="high",
    )

    assert_volume >> create_export_to_gcs_cluster >> branch_task_insert >> feed_export_increment >> feed_export_finance_increment >> delete_cluster >> end

    feed_export_increment >> clear_store_visits_trend_for_versioning_increment >> export_store_visits_trend_for_versioning_increment

    assert_volume >> create_export_to_gcs_cluster >> branch_task_insert >> feed_export_backfill >> feed_export_finance_backfill >> delete_cluster >> end

    feed_export_backfill >> clear_store_visits_trend_for_versioning_backfill >> export_store_visits_trend_for_versioning_backfill