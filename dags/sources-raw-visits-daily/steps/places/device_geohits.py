"""
DAG ID: visits_pipeline
"""
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryDeleteTableOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator


def delete_dates_partition(dag, execution_date, **context):
    from datetime import datetime

    from google.cloud import bigquery

    project_id = dag.params["project"]
    client = bigquery.Client(project=project_id)
    query_job = client.query(
        f"""
        SELECT local_date 
        FROM UNNEST(GENERATE_DATE_ARRAY(DATE_SUB("{execution_date}", INTERVAL 20 DAY), "{execution_date}")) local_date
        """,
        job_config=bigquery.QueryJobConfig(
            labels={
                "pipeline": f"{dag.dag_id}",
                "task_id": f"{context['task'].task_id[:63]}",
            }
        ),
    )
    dates = query_job.result()

    for date in dates:
        row = list(date)[0]
        year = datetime.strftime(row, "%Y")
        row_str = datetime.strftime(row, "%Y-%m-%d")
        table_id = (
            f"{dag.params['project']}.{dag.params['device_geohits_dataset']}.{year}"
        )
        query_job = client.query(
            f"""
            DELETE FROM {table_id}
            WHERE 
                local_date=CAST('{row_str}' AS DATE) AND
                append_date=CAST('{execution_date}' AS DATE)
            """,
            job_config=bigquery.QueryJobConfig(
                labels={
                    "pipeline": f"{dag.dag_id}",
                    "task_id": f"{context['task'].task_id[:63]}",
                }
            ),
        )
        query_job.result()
        print(f"Query results deleted from the table partition {table_id}${row_str}")
        print(f"{query_job.total_bytes_processed} bytes will be processed")


def write_dates_partition(dag, execution_date, mode, **context):
    from datetime import datetime

    from google.cloud import bigquery

    project_id = dag.params["project"]
    client = bigquery.Client(project=project_id)
    query_job = client.query(
        f"""
        SELECT local_date 
        FROM `{dag.params['project']}.{dag.params['device_geohits_staging_dataset']}.temp_dates_{mode}`
        """,
        job_config=bigquery.QueryJobConfig(
            labels={
                "pipeline": f"{dag.dag_id}",
                "task_id": f"{context['task'].task_id[:63]}",
            }
        ),
    )
    dates = query_job.result()

    for date in dates:
        row = list(date)[0]
        year = datetime.strftime(row, "%Y")
        row_str = datetime.strftime(row, "%Y-%m-%d")
        partition = row_str.replace("-", "")
        table_id = (
            f"{dag.params['project']}.{dag.params['device_geohits_dataset']}.{year}"
            + f"${partition}"
        )
        job_config = bigquery.QueryJobConfig(
            destination=table_id,
            write_disposition="WRITE_APPEND",
            create_disposition="CREATE_IF_NEEDED",
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY, field="local_date"
            ),
            clustering_fields=["long_lat_point", "device_id", "timezone", "sdk_ts"],
            dry_run=False,
            use_query_cache=False,
            labels={
                "pipeline": f"{dag.dag_id}",
                "task_id": f"{context['task'].task_id[:63]}",
            },
        )
        query_job = client.query(
            f"""
            SELECT 
                *,
                CAST("{execution_date}" AS DATE) AS append_date
            FROM `{dag.params['project']}.{dag.params['device_geohits_staging_dataset']}.temp_{mode}`
            WHERE local_date='{row}'
            """,
            job_config=job_config,
        )
        query_job.result()
        print(f"Query results loaded to the table partition {table_id}")
        print(f"{query_job.total_bytes_processed} bytes will be processed")

    client.delete_table(
        f"{dag.params['project']}.{dag.params['device_geohits_staging_dataset']}.temp_{mode}",
        not_found_ok=True,
    )
    client.delete_table(
        f"{dag.params['project']}.{dag.params['device_geohits_staging_dataset']}.temp_dates_{mode}",
        not_found_ok=True,
    )

    return


def register(dag, start, mode):
    """Register tasks on the dag.

    Args:
        dag (airflow.models.DAG): DAG instance to register tasks on.
        start (airflow.models.TaskInstance): Task instance all tasks will
            registered downstream from.

    Returns:
        airflow.models.TaskInstance: The last task node in this section.
    """
    query_device_geohits_temp = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="query_device_geohits_temp",
        project_id=dag.params["project"],
        configuration={
            "query": {
                "query": "{% include './bigquery/places/device_geohits.sql' %}",
                "useLegacySql": "False",
                "destinationTable": {
                    "projectId": "{{ params['project'] }}",
                    "datasetId": "{{ params['device_geohits_staging_dataset'] }}",
                    "tableId": f"temp_{mode}",
                },
                "timePartitioning": {"type": "DAY", "field": "local_date"},
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_IF_NEEDED",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )
    query_device_geohits_temp_dates = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="query_device_geohits_temp_dates",
        project_id=dag.params["project"],
        configuration={
            "query": {
                "query": (
                    '{% with staging_table="'
                    f"temp_{mode}"
                    '"%}{% include "./bigquery/places/device_geohits_temp_dates.sql" %}{% endwith %}'
                ),
                # "query": "{% include './bigquery/device_geohits_temp_dates.sql' %}"
                "useLegacySql": "False",
                "destinationTable": {
                    "projectId": "{{ params['project'] }}",
                    "datasetId": "{{ params['device_geohits_staging_dataset'] }}",
                    "tableId": f"temp_dates_{mode}",
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_IF_NEEDED",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )
    delete_monitoring_latency_geohits = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="delete_monitoring_latency_geohits",
        project_id=dag.params["project"],
        configuration={
            "query": {
                "query": "{% include './bigquery/places/monitoring/delete_latency_geohits.sql' %}",
                "useLegacySql": "False",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )
    query_monitoring_latency_geohits = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="query_monitoring_latency_geohits",
        project_id=dag.params["project"],
        configuration={
            "query": {
                "query": (
                    '{% with staging_table="'
                    f"temp_{mode}"
                    '"%}{% include "./bigquery/places/monitoring/latency_geohits.sql" %}{% endwith %}'
                ),
                "useLegacySql": "False",
                "destinationTable": {
                    "projectId": "{{ params['project'] }}",
                    "datasetId": "{{ params['metrics_dataset'] }}",
                    "tableId": "{{ params['geohits_latency_table'] }}",
                },
                "writeDisposition": "WRITE_APPEND",
                "createDisposition": "CREATE_IF_NEEDED",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )
    delete_dates_partition_geohits = PythonOperator(
        task_id="delete_write_dates_partition_geohits",
        python_callable=delete_dates_partition,
        op_kwargs={"dag": dag, "execution_date": "{{ ds }}"},
        dag=dag,
    )
    query_write_dates_partition_geohits = PythonOperator(
        task_id="query_write_dates_partition_geohits",
        python_callable=write_dates_partition,
        op_kwargs={"dag": dag, "execution_date": "{{ ds }}", "mode": mode},
        dag=dag,
    )
    delete_monitoring_latency_raw_tamoco = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="delete_monitoring_latency_raw_tamoco",
        project_id=dag.params["project"],
        configuration={
            "query": {
                "query": "{% include './bigquery/places/monitoring/delete_latency_raw_tamoco.sql' %}",
                "useLegacySql": "False",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )
    query_monitoring_latency_raw_tamoco = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="query_monitoring_latency_raw_tamoco",
        project_id=dag.params["project"],
        configuration={
            "query": {
                "query": "{% include './bigquery/places/monitoring/latency_raw_tamoco.sql' %}",
                "useLegacySql": "False",
                "destinationTable": {
                    "projectId": "{{ params['project'] }}",
                    "datasetId": "{{ params['metrics_dataset'] }}",
                    "tableId": "{{ params['tamoco_latency_table'] }}",
                },
                "writeDisposition": "WRITE_APPEND",
                "createDisposition": "CREATE_IF_NEEDED",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )
    delete_test_raw_to_geohits = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="delete_test_raw_to_geohits",
        project_id=dag.params["project"],
        configuration={
            "query": {
                "query": "{% include './bigquery/places/tests/delete_raw_to_geohits.sql' %}",
                "useLegacySql": "False",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )
    query_test_raw_to_geohits = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="query_test_raw_to_geohits",
        project_id=dag.params["project"],
        configuration={
            "query": {
                "query": "{% include './bigquery/places/tests/raw_to_geohits.sql' %}",
                "useLegacySql": "False",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )
    delete_raw_data_table = BigQueryDeleteTableOperator(
        task_id="delete_raw_data_table",
        deletion_dataset_table="{{ params['project'] }}.{{ params['raw_data_dataset'] }}.{{ execution_date.strftime('%Y') }}",
        dag=dag,
        location="EU",
    )
    move_standard_to_archive = GCSToGCSOperator(
        task_id="move_standard_to_archive",
        source_bucket="{{ params['raw_data_bucket'] }}",
        source_object="{{ execution_date.strftime('%Y/%m/%d') }}/*.parquet",
        destination_bucket="{{ params['raw_archive_data_bucket'] }}",
        destination_object="{{ execution_date.strftime('%Y/%m/%d') }}/",
        move_object=True,
        dag=dag,
    )
    (
        start
        >> query_device_geohits_temp
        >> query_device_geohits_temp_dates
        >> delete_monitoring_latency_geohits
        >> query_monitoring_latency_geohits
        >> delete_dates_partition_geohits
        >> query_write_dates_partition_geohits
        >> delete_monitoring_latency_raw_tamoco
        >> query_monitoring_latency_raw_tamoco
        >> delete_test_raw_to_geohits
        >> query_test_raw_to_geohits
        >> delete_raw_data_table
        >> move_standard_to_archive
    )

    return query_write_dates_partition_geohits

    # return delete_raw_data_table
