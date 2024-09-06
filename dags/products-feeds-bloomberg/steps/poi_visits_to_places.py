"""
DAG ID: sg_visits_pipeline
"""
from datetime import datetime

from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from google.cloud import bigquery


def write_dates_partition(dag, **context):
    project_id = dag.params["project"]
    client = bigquery.Client(project=project_id)
    query_job = client.query(
        f"""
        SELECT local_date
        FROM `{dag.params['project']}.{dag.params['bloomberg_dataset']}.temp_dates_{context["execution_date"].strftime('%Y-%m-%d')}`
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
            f"{dag.params['project']}.{dag.params['bloomberg_dataset']}.{year}"
            + f"${partition}"
        )
        job_config = bigquery.QueryJobConfig(
            destination=table_id,
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_APPEND",
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY, field="local_date"
            ),
            clustering_fields=["placekey", "brand_name", "fk_met_areas"],
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
                fk_met_areas,
                visit_start_ts,
                visit_ts_end,
                device_id,
                device_os,
                duration,
                local_date,
                local_hour,
                day_of_week,
                visit_score,
                visit_score_opening,
                lat_visit_point,
                long_visit_point,
                placekey,
                street_address,
                city,
                region,
                name,
                poi_lat,
                poi_long,
                naics_code,
                brand_name,
                top_category,
                sub_category
            FROM
                `{dag.params['project']}.{dag.params['bloomberg_dataset']}.temp_{context["execution_date"].strftime('%Y-%m-%d')}`
            WHERE
                local_date='{row}'
            """,
            job_config=job_config,
        )  # --> removed _dynamic when merged
        query_job.result()
        print(f"Query results loaded to the table partition {table_id}")
        print(f"{query_job.total_bytes_processed} bytes will be processed")

    client.delete_table(
        f"{dag.params['project']}.{dag.params['bloomberg_dataset']}.temp_{context['execution_date'].strftime('%Y-%m-%d')}",
        not_found_ok=True,
    )
    client.delete_table(
        f"{dag.params['project']}.{dag.params['bloomberg_dataset']}.temp_dates_{context['execution_date'].strftime('%Y-%m-%d')}",
        not_found_ok=True,
    )
    return


def register(dag, start):
    """Register tasks on the dag.

    Args:
        dag (airflow.models.DAG): DAG instance to register tasks on.
        start (airflow.models.TaskInstance): Task instance all tasks will
            registered downstream from.

    Returns:
        airflow.models.TaskInstance: The last task node in this section.
    """
    poi_visits_to_places_temp = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="poi_visits_to_places_temp",
        project_id=f"{dag.params['project']}",
        configuration={
            "query": {
                "query": "{% include './bigquery/poi_visits_to_places.sql' %}",
                "useLegacySql": "False",
                "destinationTable": {
                    "projectId": "{{ params['project'] }}",
                    "datasetId": "{{ params['bloomberg_dataset'] }}",
                    "tableId": "temp_{{ ds }}",  # --> removed _dynamic when merged
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

    poi_visits_to_places_temp_dates = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="poi_visits_to_places_temp_dates",
        project_id=f"{dag.params['project']}",
        configuration={
            "query": {
                "query": "{% include './bigquery/poi_visits_to_places_temp_dates.sql' %}",
                "useLegacySql": "False",
                "destinationTable": {
                    "projectId": "{{ params['project'] }}",
                    "datasetId": "{{ params['bloomberg_dataset'] }}",
                    "tableId": "temp_dates_{{ ds }}",
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

    poi_visits_to_places_write_dates_partition = PythonOperator(
        task_id="poi_visits_to_places_write_dates_partition",
        python_callable=write_dates_partition,
        op_kwargs={
            "dag": dag,
        },
        dag=dag,
        provide_context=True,
    )

    (
        start
        >> poi_visits_to_places_temp
        >> poi_visits_to_places_temp_dates
        >> poi_visits_to_places_write_dates_partition
    )

    return poi_visits_to_places_write_dates_partition
