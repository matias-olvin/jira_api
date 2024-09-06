"""
DAG ID: smc_visits_daily_backfill
"""

from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


def naics_filter_partition(dag, **context):
    from google.cloud import bigquery

    bq_client = bigquery.Client()
    year = (
        context["execution_date"]
        .date()
        .subtract(days=int(Variable.get("latency_days_visits")))
        .strftime("%Y")
    )
    logical_date = (
        context["execution_date"]
        .date()
        .subtract(days=int(Variable.get("latency_days_visits")))
        .strftime("%Y-%m-%d")
    )
    logical_date_plus_1 = (
        context["execution_date"]
        .date()
        .subtract(days=(int(Variable.get("latency_days_visits")) - 1))
        .strftime("%Y-%m-%d")
    )
    query_job = bq_client.query(
        f"""
            DELETE
            `{Variable.get('env_project')}.{dag.params['poi_visits_scaled_dataset']}.{year}` i
            WHERE
            local_date >= DATE('{logical_date}') AND 
            local_date < DATE('{logical_date_plus_1}') AND
            i.naics_code IN 
            (SELECT naics_code 
            from 
            `{Variable.get('env_project')}.{dag.params['base_table_dataset']}.{dag.params['naics_code_subcategories']}` 
            WHERE exclude_bool=TRUE)
            """,
        job_config=bigquery.QueryJobConfig(
            labels={
                "pipeline": f"{dag.dag_id}",
                "task_id": f"{context['task'].task_id[:63]}",
            }
        ),
    )
    query_job.result()
    bq_client.delete_table(
        f"{Variable.get('env_project')}.{dag.params['smc_poi_visits_staging_dataset']}.{dag.params['block_1_output_table']}_{context['execution_date'].strftime('%Y-%m-%d')}",
        not_found_ok=True,
    )


def register(dag, start):
    """Register tasks on the dag.

    Args:
        dag (airflow.models.DAG): DAG instance to register tasks on.
        start (airflow.models.TaskInstance): Task instance all tasks will
            registered downstream from.

    Returns:
        airflow.models.TaskInstance: The last task node in this section.
    """
    delete_poi_visits_scaled = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="delete_poi_visits_scaled",
        project_id=Variable.get("env_project"),
        configuration={
            "query": {
                "query": "DELETE `{{ var.value.env_project }}.{{ params['poi_visits_scaled_dataset'] }}.{{ execution_date.subtract(days=var.value.latency_days_visits|int).strftime('%Y') }}` where local_date = DATE_ADD( DATE('{{ ds }}'), INTERVAL - {{ var.value.latency_days_visits|int }} DAY )",
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
    query_poi_visits_scaled = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="query_poi_visits_scaled",
        project_id=Variable.get("env_project"),
        configuration={
            "query": {
                "query": "{% include './bigquery/places/poi_visits_scaled.sql' %}",
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
    delete_day_stats_visits_scaled = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="delete_day_stats_visits_scaled",
        project_id=Variable.get("env_project"),
        configuration={
            "query": {
                "query": "DELETE `{{ var.value.env_project }}.{{ params['smc_metrics_dataset'] }}.{{ params['day_stats_visits_scaled_table'] }}` where local_date = DATE_ADD( DATE('{{ ds }}'), INTERVAL -{{ var.value.latency_days_visits }} DAY )",
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
    query_day_stats_visits_scaled = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="query_day_stats_visits_scaled",
        project_id=Variable.get("env_project"),
        configuration={
            "query": {
                "query": "{% include './bigquery/places/scaling/daily_stats_visits_scaled.sql' %}",
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
    query_naics_filtering = PythonOperator(
        task_id="query_naics_filtering",
        python_callable=naics_filter_partition,
        op_kwargs={
            "dag": dag,
        },
        provide_context=True,
        dag=dag,
    )

    (
        start
        >> delete_poi_visits_scaled
        >> query_poi_visits_scaled
        >> delete_day_stats_visits_scaled
        >> query_day_stats_visits_scaled
        >> query_naics_filtering
    )

    return query_naics_filtering