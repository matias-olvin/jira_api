"""
DAG ID: sg_visits_pipeline
"""
import time
from datetime import datetime

from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryValueCheckOperator,
)


def register(dag, start):
    """Register tasks on the dag.

    Args:
        dag (airflow.models.DAG): DAG instance to register tasks on.
        start (airflow.models.TaskInstance): Task instance all tasks will
            registered downstream from.
        mode (str): "update" or "backfill"

    Returns:
        airflow.models.TaskInstance: The last task node in this section.
    """
    delete_day_stats_visits = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="delete_day_stats_visits",
        project_id=Variable.get("env_project"),
        configuration={
            "query": {
                "query": "DELETE `{{ var.value.env_project }}.{{ params['smc_metrics_dataset'] }}.{{ params['day_stats_visits_table'] }}` WHERE local_date = DATE_ADD( DATE('{{ ds }}'), INTERVAL -{{ var.value.latency_days_visits }} DAY )",
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
    query_day_stats_visits = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="query_day_stats_visits",
        project_id=Variable.get("env_project"),
        configuration={
            "query": {
                "query": "{% include './bigquery/places/scaling/daily_stats_visits.sql' %}",
                "useLegacySql": "False",
                "destinationTable": {
                    "projectId": "{{ var.value.env_project }}",
                    "datasetId": "{{ params['smc_metrics_dataset'] }}",
                    "tableId": "{{ params['day_stats_visits_table'] }}",
                },
                "timePartitioning": {
                    "field": "local_date",
                    "type": "DAY",
                },
                "clustering": {"fields": ["s2_token", "part_of_day", "publisher_id"]},
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
    delete_quality_ml_input = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="delete_quality_ml_input",
        project_id=Variable.get("env_project"),
        configuration={
            "query": {
                "query": "DELETE `{{ var.value.env_project }}.{{ params['quality_dataset'] }}.{{ params['model_input_complete_table'] }}` WHERE local_date = DATE_ADD( DATE('{{ ds }}'), INTERVAL -{{ var.value.latency_days_visits }} DAY )",
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

    query_quality_ml_input = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="query_quality_ml_input",
        project_id=Variable.get("env_project"),
        configuration={
            "query": {
                "query": "{% include './bigquery/places/scaling/quality_ml_input.sql' %}",
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

    delete_block_1 = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="delete_block_1",
        project_id=Variable.get("env_project"),
        configuration={
            "query": {
                "query": "DROP TABLE IF EXISTS `{{ var.value.env_project }}.{{ params['smc_poi_visits_staging_dataset'] }}.{{ params['block_1_output_table'] }}_{{ ds }}` ",
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

    query_block_1 = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="query_block_1",
        project_id=Variable.get("env_project"),
        configuration={
            "query": {
                "query": "{% include './bigquery/places/scaling/block_1.sql' %}",
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
    delete_daily_estimation_factor = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="delete_daily_estimation_factor",
        project_id=Variable.get("env_project"),
        configuration={
            "query": {
                "query": "DELETE `{{ var.value.env_project }}.{{ params['smc_daily_estimation_dataset'] }}.{{ params['daily_factor_table'] }}` WHERE local_date = DATE_ADD( DATE('{{ ds }}'), INTERVAL -{{ var.value.latency_days_visits }} DAY )",
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

    delete_daily_olvin = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="delete_daily_olvin",
        project_id=Variable.get("env_project"),
        configuration={
            "query": {
                "query": "DELETE `{{ var.value.env_project }}.{{ params['smc_daily_estimation_dataset'] }}.{{ params['grouped_daily_olvin_table'] }}` WHERE local_date = DATE_ADD( DATE('{{ ds }}'), INTERVAL -{{ var.value.latency_days_visits }} DAY )",
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

    query_daily_olvin = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="query_daily_olvin",
        project_id=Variable.get("env_project"),
        configuration={
            "query": {
                "query": "{% include './bigquery/places/scaling/update_daily_olvin.sql' %}",
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

    delete_run = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="delete_run",
        configuration={
            "query": {
                "query": f"""
                        DELETE FROM
                            `{{{{ var.value.env_project }}}}.{{{{ params['accessible_by_sns_dataset'] }}}}.{{{{ params['daily_estimation_runs_table'] }}}}`
                        WHERE
                            local_date = date_sub(DATE('{{{{ ds }}}}') , interval {{{{ var.value.latency_days_visits }}}} day)
                    """,
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

    trigger_daily_estimation = BashOperator(
        task_id="trigger_daily_estimation",
        bash_command=f"gcloud composer environments run prod-sensormatic "
        f"--project {dag.params['sns_project']} "
        "--location europe-west1 "
        f"--impersonate-service-account {dag.params['cross_project_service_account']} "
        f'dags trigger -- -e "{{{{ ds }}}} {datetime.now().time()}" smc_daily_estimation_backfill '
        "|| true ",  # this line is needed due to gcloud bug.
        dag=dag,
    )

    check_completion = BigQueryValueCheckOperator(
gcp_conn_id="google_cloud_olvin_default",
        sql=f"SELECT run_completion FROM `{Variable.get('env_project')}."
        f"{dag.params['accessible_by_sns_dataset']}.{dag.params['daily_estimation_runs_table']}` "
        f"WHERE local_date = '{{{{ execution_date.subtract(days=var.value.latency_days_visits|int).strftime('%Y-%m-%d') }}}}' ",
        pass_value=True,
        task_id="check_completion",
        dag=dag,
        location="EU",
        use_legacy_sql=False,
        depends_on_past=False,
        wait_for_downstream=False,
        retries=5,
    )

    load_daily_estimation_factor_from_sns = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="load_daily_estimation_factor_from_sns",
        project_id=Variable.get("env_project"),
        configuration={
            "query": {
                "query": "{% include './bigquery/places/scaling/load_daily_factor.sql' %}",
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

    (
        start
        >> delete_day_stats_visits
        >> query_day_stats_visits
        >> delete_quality_ml_input
        >> query_quality_ml_input
        >> delete_block_1
        >> query_block_1
        >> delete_daily_estimation_factor
        >> delete_daily_olvin
        >> query_daily_olvin
        >> delete_run
        >> trigger_daily_estimation
        >> check_completion
        >> load_daily_estimation_factor_from_sns
    )

    return load_daily_estimation_factor_from_sns
