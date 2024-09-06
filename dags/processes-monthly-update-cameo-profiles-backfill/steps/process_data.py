"""
DAG ID: demographics_pipeline
"""
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from dateutil.relativedelta import relativedelta
from airflow.models import Variable


def register(dag, start):
    # Query all monthly visits
    from datetime import date

    import pandas as pd

    dates_compute = pd.date_range(
        "2018-10-01",
        Variable.get("smc_end_date"),
        freq="MS",
        inclusive="left",
    )

    write_disposition = "WRITE_TRUNCATE"
    query_cameo_visits_old = start
    for timestamp_start, timestamp_end in zip(dates_compute[:-1], dates_compute[1:]):
        date_start = timestamp_start.strftime("%Y-%m-01")
        date_end = timestamp_end.strftime("%Y-%m-01")

        query_cameo_visits = BigQueryInsertJobOperator(
            task_id=f"query_cameo_visits_{date_start}",
            configuration={
                "query": {
                    "query": "{% include './bigquery/raw_cameo_visits.sql' %}",
                    "useLegacySql": "False",
                    "writeDisposition": write_disposition,
                    "createDisposition": "CREATE_IF_NEEDED",
                    "timePartitioning": {
                        "type": "DAY",
                        "field": "local_date",
                    },
                    "clustering": {"fields": ["fk_sgplaces", "CAMEO_USA"]},
                    "destinationTable": {
                    "projectId": "{{ params['storage-prod'] }}",
                    "datasetId": "{{ params['cameo_staging_dataset'] }}",
                    "tableId": "{{ params['cameo_visits_table'] }}",
                },
                },
                "labels": {"pipeline": "{{ params['pipeline_label_value'] }}"},
            },
            params={
                "date_start": date_start,
                "date_end": date_end,
            },
            dag=dag,
            location="EU",
        )
        query_cameo_visits_old >> query_cameo_visits
        query_cameo_visits_old = query_cameo_visits
        write_disposition = "WRITE_APPEND"

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
        params={
            "date_start": date_start,
        },
    )
    query_cameo_visits_old >> query_check_cameo_visits

    query_hann = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="query_hann",
        configuration={
            "query": {
                "query": "{% include './bigquery/hann.sql' %}",
                "useLegacySql": "False",
            },
            "labels": {"pipeline": "{{ params['pipeline_label_value'] }}"},
        },
        dag=dag,
        depends_on_past=True,
        location="EU",
    )
    query_check_cameo_visits >> query_hann

    query_ema = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="query_ema",
        configuration={
            "query": {
                "query": "{% include './bigquery/ema.sql' %}",
                "useLegacySql": "False",
            },
            "labels": {"pipeline": "{{ params['pipeline_label_value'] }}"},
        },
        dag=dag,
        depends_on_past=True,
        location="EU",
    )
    query_check_cameo_visits >> query_ema

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
    [query_ema, query_hann] >> query_final_smoothed

    return query_final_smoothed
