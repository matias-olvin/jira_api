"""
DAG ID: visits_pipeline
"""
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


def delete_dates_partition(dag, execution_date, mode, **context):
    from datetime import datetime

    from google.cloud import bigquery

    project_id = dag.params["project"]
    client = bigquery.Client(project=project_id)
    query_job = client.query(
        f"""
        SELECT local_date 
        FROM `{dag.params['project']}.{dag.params['device_clusters_staging_dataset']}.temp_dates_{mode}`
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
            f"{dag.params['project']}.{dag.params['device_clusters_dataset']}.{year}"
        )
        query_job = client.query(
            f"""
            DELETE FROM {table_id}
            WHERE 
                local_date = CAST('{row_str}' AS DATE) AND
                append_date = CAST('{execution_date}' AS DATE)
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
        FROM `{dag.params['project']}.{dag.params['device_clusters_staging_dataset']}.temp_dates_{mode}`
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
            f"{dag.params['project']}.{dag.params['device_clusters_dataset']}.{year}"
            + f"${partition}"
        )
        job_config = bigquery.QueryJobConfig(
            destination=table_id,
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_APPEND",
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY, field="local_date"
            ),
            clustering_fields=[
                "lat_long_visit_point",
                "country",
                "device_id",
                "publisher_id",
            ],
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
            FROM `{dag.params['project']}.{dag.params['device_clusters_staging_dataset']}.temp_{mode}`
            WHERE local_date='{row}'
            """,
            job_config=job_config,
        )
        query_job.result()
        print(f"Query results loaded to the table partition {table_id}")
        print(f"{query_job.total_bytes_processed} bytes will be processed")

    client.delete_table(
        f"{dag.params['project']}.{dag.params['device_clusters_staging_dataset']}.temp_{mode}",
        not_found_ok=True,
    )
    client.delete_table(
        f"{dag.params['project']}.{dag.params['device_clusters_staging_dataset']}.temp_dates_{mode}",
        not_found_ok=True,
    )

    return


def register(dag, start, mode):
    """
    Register tasks on the dag.

    Args:
        dag (airflow.models.DAG): DAG instance to register tasks on.
        start (airflow.models.TaskInstance): Task instance all tasks will
            registered downstream from.

    Returns:
        airflow.models.TaskInstance: The last task node in this section.
    """
    # UNIVERSAL TASKS
    query_device_clusters_temp = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="query_device_clusters_temp",
        project_id=dag.params["project"],
        configuration={
            "query": {
                "query": "{% include './bigquery/places/device_clusters.sql' %}",
                "useLegacySql": "False",
                "destinationTable": {
                    "projectId": "{{ params['project'] }}",
                    "datasetId": "{{ params['device_clusters_staging_dataset'] }}",
                    "tableId": f"temp_{mode}",
                },
                "timePartitioning": {"type": "DAY", "field": "local_date"},
                "clustering": {
                    "fields": [
                        "lat_long_visit_point",
                        "country",
                        "device_id",
                        "publisher_id",
                    ]
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
    query_device_clusters_temp_dates = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="query_device_clusters_temp_dates",
        project_id=dag.params["project"],
        configuration={
            "query": {
                "query": (
                    '{% with staging_table="'
                    f"temp_{mode}"
                    '"%}{% include "./bigquery/places/device_clusters_temp_dates.sql" %}{% endwith %}'
                ),
                "useLegacySql": "False",
                "destinationTable": {
                    "projectId": "{{ params['project'] }}",
                    "datasetId": "{{ params['device_clusters_staging_dataset'] }}",
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
    delete_dates_partition_cluster = PythonOperator(
        task_id="delete_dates_partition_cluster",
        python_callable=delete_dates_partition,
        op_kwargs={"dag": dag, "execution_date": "{{ ds }}", "mode": mode},
        dag=dag,
    )
    query_write_dates_partition_cluster = PythonOperator(
        task_id="query_write_dates_partition_cluster",
        python_callable=write_dates_partition,
        op_kwargs={"dag": dag, "execution_date": "{{ ds }}", "mode": mode},
        dag=dag,
    )
    export_device_geohits = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="export_device_geohits",
        project_id=dag.params["project"],
        configuration={
            "extract": {
                "destinationUri": (
                    (
                        "gs://device-geohits-prod-olvin-com/"
                        "{{ execution_date.subtract(days=20).strftime('%Y') }}/"
                        "{{ execution_date.subtract(days=20).strftime('%m') }}/"
                        "{{ execution_date.subtract(days=20).strftime('%d') }}/device_geohits-*.json.gz"
                    )
                ),
                "destinationFormat": "NEWLINE_DELIMITED_JSON",
                "compression": "GZIP",
                "sourceTable": {
                    "projectId": "{{ params['project'] }}",
                    "datasetId": "{{ params['device_geohits_dataset'] }}",
                    "tableId": "{{ execution_date.subtract(days=20).strftime('%Y') }}${{ execution_date.subtract(days=20).strftime('%Y%m%d') }}",
                },
            },
            "labels": {
                "pipeline": "{{ dag.dag_id }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
    )
    delete_from_device_geohits = BashOperator(
        task_id="delete_from_device_geohits",
        bash_command="bq rm -f {{ params['project'] }}:{{ params['device_geohits_dataset'] }}.{{ execution_date.subtract(days=20).strftime('%Y') }}\${{ execution_date.subtract(days=20).strftime('%Y%m%d') }}",
        dag=dag,
    )
    (
        start
        >> query_device_clusters_temp
        >> query_device_clusters_temp_dates
        >> delete_dates_partition_cluster
        >> query_write_dates_partition_cluster
        >> export_device_geohits
        >> delete_from_device_geohits
    )

    return query_write_dates_partition_cluster
