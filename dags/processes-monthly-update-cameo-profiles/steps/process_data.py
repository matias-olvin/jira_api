"""
DAG ID: demographics_pipeline
"""
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryValueCheckOperator,
)


def register(dag, start):
    query_cameo_visits = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="query_cameo_visits",
        configuration={
            "query": {
                "query": "{% include './bigquery/raw_cameo_visits.sql' %}",
                "useLegacySql": "False",
            },
            "labels": {"pipeline": "{{ params['pipeline_label_value'] }}"},
        },
        dag=dag,
        location="EU",
    )
    start >> query_cameo_visits

    query_check_cameo_visits = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="query_check_cameo_visits",
        configuration={
            "query": {
                "query": "{% include './bigquery/verification/cameo_visits.sql' %}",
                "useLegacySql": "False",
            },
            "labels": {"pipeline": "{{ params['pipeline_label_value'] }}"},
        },
        dag=dag,
        location="EU",
    )
    query_cameo_visits >> query_check_cameo_visits

    query_check_cameo_visits_postgres = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="query_check_cameo_visits_postgres",
        configuration={
            "query": {
                "query": "{% include './bigquery/verification/cameo_visits_postgres.sql' %}",
                "useLegacySql": "False",
            },
            "labels": {"pipeline": "{{ params['pipeline_label_value'] }}"},
        },
        dag=dag,
        location="EU",
    )
    query_cameo_visits >> query_check_cameo_visits_postgres

    delete_ema_forward = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="delete_ema_forward",
        configuration={
            "query": {
                "query": "DELETE FROM `{{ params['storage-prod'] }}.{{ params['cameo_staging_dataset'] }}"
                ".{{ params['cameo_ema_24_table'] }}`"
                "WHERE local_date >= DATE_TRUNC('{{ ds }}', MONTH) "
                "AND local_date < DATE_TRUNC(DATE_ADD('{{ ds }}', INTERVAL 1 MONTH), MONTH)",
                "useLegacySql": "False",
            },
            "labels": {"pipeline": "{{ params['pipeline_label_value'] }}"},
        },
        dag=dag,
        location="EU",
    )
    [query_check_cameo_visits, query_check_cameo_visits_postgres] >> delete_ema_forward

    query_ema_forward = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="query_ema_forward",
        configuration={
            "query": {
                "query": "{% include './bigquery/ema_forward.sql' %}",
                "useLegacySql": "False",
            },
            "labels": {"pipeline": "{{ params['pipeline_label_value'] }}"},
        },
        dag=dag,
        depends_on_past=True,
        location="EU",
    )
    delete_ema_forward >> query_ema_forward

    delete_final_smoothed = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="delete_final_smoothed",
        configuration={
            "query": {
                "query": "DELETE FROM `{{ params['storage-prod'] }}.{{ params['cameo_staging_dataset'] }}"
                ".{{ params['smoothed_transition_table'] }}`"
                "WHERE local_date >= DATE_TRUNC('{{ ds }}', MONTH) "
                "AND local_date < DATE_TRUNC(DATE_ADD('{{ ds }}', INTERVAL 1 MONTH), MONTH)",
                "useLegacySql": "False",
            },
            "labels": {"pipeline": "{{ params['pipeline_label_value'] }}"},
        },
        dag=dag,
        location="EU",
    )
    query_ema_forward >> delete_final_smoothed

    query_final_smoothed = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="query_final_smoothed",
        configuration={
            "query": {
                "query": "{% include './bigquery/transition_smoothing.sql' %}",
                "useLegacySql": "False",
            },
            "labels": {"pipeline": "{{ params['pipeline_label_value'] }}"},
        },
        dag=dag,
        location="EU",
    )
    delete_final_smoothed >> query_final_smoothed

    check_final_smoothed = BigQueryValueCheckOperator(
gcp_conn_id="google_cloud_olvin_default",
        sql="""
             SELECT COUNTIF(num_rows IS NULL)
            FROM (
              SELECT DATE_TRUNC(DATE(date), MONTH) AS local_date
              FROM UNNEST(GENERATE_DATE_ARRAY(DATE('2019-01-01'), "{{ ds }}", INTERVAL 1 MONTH)) AS date
            )
            LEFT JOIN (
              SELECT local_date, count(*) as num_rows
              FROM
              `{{ params['storage-prod'] }}.{{ params['cameo_staging_dataset'] }}.{{ params['smoothed_transition_table'] }}`
              GROUP BY local_date
            )
            USING (local_date)
        """,
        pass_value=0,
        task_id="check_smooth_transition_empty_months",
        dag=dag,
        location="EU",
        use_legacy_sql=False,
        depends_on_past=False,
        wait_for_downstream=False,
    )
    query_final_smoothed >> check_final_smoothed

    query_postgres_table = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="query_postgres_table",
        configuration={
            "query": {
                "query": "{% include './bigquery/postgres_formatting.sql' %}",
                "useLegacySql": "False",
            },
            "labels": {"pipeline": "{{ params['pipeline_label_value'] }}"},
        },
        dag=dag,
        location="EU",
    )
    check_final_smoothed >> query_postgres_table

    query_postgres_monthly_table = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="query_postgres_monthly_table",
        configuration={
            "query": {
                "query": "{% include './bigquery/split_postgres_monthly.sql' %}",
                "useLegacySql": "False",
            },
            "labels": {"pipeline": "{{ params['pipeline_label_value'] }}"},
        },
        dag=dag,
        location="EU",
    )
    query_postgres_table >> query_postgres_monthly_table

    monitoring_current_month_check = BigQueryValueCheckOperator(
gcp_conn_id="google_cloud_olvin_default",
        task_id="monitoring_current_month_check",
        retries=1,
        sql="{% include './bigquery/verification/cameo_monthly.sql' %}",
        use_legacy_sql=False,
        pass_value=False,
        location="EU",
        dag=dag,
    )
    query_postgres_monthly_table >> monitoring_current_month_check

    return monitoring_current_month_check
