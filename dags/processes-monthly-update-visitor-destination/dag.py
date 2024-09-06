"""
DAG ID: sg_agg_stats
"""
import os, pendulum
from datetime import datetime, timedelta
from importlib.machinery import SourceFileLoader
from common.utils import dag_args, callbacks

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryValueCheckOperator,
)
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.utils.weight_rule import WeightRule
from dateutil.relativedelta import relativedelta
from common.utils import slack_users

# Relative imports based on the current file location
path = f"{os.path.dirname(os.path.realpath(__file__))}"
steps_path = path + "/steps/"
# send_2_posgres = SourceFileLoader("send_2_posgres", steps_path + "send_2_postgres.py").load_module()


# Define the dates for computation
def prev_month_formatted(execution_date):
    return execution_date.subtract(months=1).format("%Y-%m-01")


def run_month_formatted(execution_date):
    return execution_date.add(months=1).format("%Y-%m-01")


def next_run_month_formatted(execution_date):
    return execution_date.add(months=2).format("%Y-%m-01")


def first_month_formatted(_):
    return "2019-01-01"


def first_buffer_month_formatted(_):
    return "2019-04-01"



DAG_ID = path.split("/")[-1]

env_args = dag_args.make_env_args(
    dag_id=DAG_ID,
)

# Create monthly DAG
with DAG(
    env_args["dag_id"],
    default_args=dag_args.make_default_args(
    start_date=pendulum.datetime(2022, 7, 1, tz="Europe/London"),
    retries=0,
    on_failure_callback=callbacks.task_fail_slack_alert(slack_users.CARLOS),
    on_retry_callback=callbacks.task_retry_slack_alert(slack_users.CARLOS),
    weight_rule=WeightRule.UPSTREAM,
),
    concurrency=4,
    schedule_interval=env_args["schedule_interval"],
    tags=[env_args["env"], "monthly"],
    params=dag_args.load_config(__file__),
    # doc_md=dag_args.load_docs(__file__),
    user_defined_macros={},
) as dag:
    start = DummyOperator(task_id="start", dag=dag)

    observed_conns_last_month = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="create_observed_connections_table",
        configuration={
            "query": {
                "query": f"{{% include './bigquery/create_observed_connections_table.sql' %}}",
                "useLegacySql": "False",
                "destinationTable": {
                    "projectId": "{{ params['storage-prod'] }}",
                    "datasetId": "{{ params['visitor_brand_destinations_monthly_dataset'] }}",
                    "tableId": "{{ execution_date.add(months=1).strftime('%Y') }}",
                },
                "timePartitioning": {"type": "MONTH", "field": "local_date"},
                "clustering": {"fields": ["fk_sgplaces", "fk_sgbrands"]},
                "writeDisposition": "WRITE_APPEND",
                "createDisposition": "CREATE_IF_NEEDED",
                "labels": {"pipeline": "{{ params['pipeline_label_value'] }}"},
            }
        },
        dag=dag,
        location="EU",
    )

    observed_conns_last_year = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="aggregate_observed_connections_last_year_table",
        configuration={
            "query": {
                "query": f"{{% include './bigquery/aggregate_last_12_months.sql' %}}",
                "useLegacySql": "False",
            },
            "labels": {"pipeline": "{{ params['pipeline_label_value'] }}"},
        },
        dag=dag,
        location="EU",
    )

    monitoring_observed_conns_query = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="monitoring_observed_conns_query",
        configuration={
            "query": {
                "query": f"{{% include './bigquery/tests/observed_connections.sql' %}}",
                "useLegacySql": "False",
            },
            "labels": {"pipeline": "{{ params['pipeline_label_value'] }}"},
        },
        dag=dag,
        location="EU",
    )

    monitoring_observed_conns_check = BigQueryValueCheckOperator(
gcp_conn_id="google_cloud_olvin_default",
        task_id="monitoring_observed_conns_check",
        retries=1,
        sql="SELECT terminate FROM `{{params['storage-prod']}}.{{params['visitor_brand_destinations_dataset']}}.{{params['monitoring_observed_connections_table']}}` WHERE month_update = FORMAT_DATE('%B', DATE_TRUNC(DATE_SUB( DATE('{{ds}}'), INTERVAL 0 MONTH), MONTH)  )",
        use_legacy_sql=False,
        pass_value=False,
        location="EU",
        dag=dag,
    )

    monitoring_total_devices_check = BigQueryValueCheckOperator(
gcp_conn_id="google_cloud_olvin_default",
        task_id="monitoring_total_devices_check",
        retries=1,
        sql="SELECT max(diff) FROM (SELECT max(total_devices)-min(total_devices) as diff FROM  `{{ params['storage-prod'] }}.{{ params['visitor_brand_destinations_dataset'] }}.{{ params['observed_connections_table'] }}` GROUP BY fk_sgplaces)",
        use_legacy_sql=False,
        pass_value=0,
        location="EU",
        dag=dag,
    )

    monitoring_leakage_query = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="monitoring_leakage_query",
        configuration={
            "query": {
                "query": f"{{% include './bigquery/metrics/test_leakage_ferber.sql' %}}",
                "useLegacySql": "False",
                "destinationTable": {
                    "projectId": "{{ params['storage-prod'] }}",
                    "datasetId": "{{ params['visitor_brand_destinations_metrics_dataset'] }}",
                    "tableId": "{{ params['monitoring_leakage_table'] }}",
                },
                "writeDisposition": "WRITE_APPEND",
                "createDisposition": "CREATE_IF_NEEDED",
            },
            "labels": {"pipeline": "{{ params['pipeline_label_value'] }}"},
        },
        dag=dag,
        location="EU",
    )

    monitoring_leakage_check = BigQueryValueCheckOperator(
gcp_conn_id="google_cloud_olvin_default",
        task_id="monitoring_leakage_check",
        retries=1,
        sql="SELECT terminate FROM `{{params['storage-prod']}}.{{params['visitor_brand_destinations_metrics_dataset']}}.{{params['monitoring_leakage_table']}}` WHERE run_date = DATE('{{ds}}')",
        use_legacy_sql=False,
        pass_value=False,
        location="EU",
        dag=dag,
    )

    monitoring_synergy_query = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="monitoring_synergy_query",
        configuration={
            "query": {
                "query": f"{{% include './bigquery/metrics/test_synergy_ferber.sql' %}}",
                "useLegacySql": "False",
                "destinationTable": {
                    "projectId": "{{ params['storage-prod'] }}",
                    "datasetId": "{{ params['visitor_brand_destinations_metrics_dataset'] }}",
                    "tableId": "{{ params['monitoring_synergy_table'] }}",
                },
                "writeDisposition": "WRITE_APPEND",
                "createDisposition": "CREATE_IF_NEEDED",
            },
            "labels": {"pipeline": "{{ params['pipeline_label_value'] }}"},
        },
        dag=dag,
        location="EU",
    )

    monitoring_synergy_check = BigQueryValueCheckOperator(
gcp_conn_id="google_cloud_olvin_default",
        task_id="monitoring_synergy_check",
        retries=1,
        sql="SELECT terminate FROM `{{params['storage-prod']}}.{{params['visitor_brand_destinations_metrics_dataset']}}.{{params['monitoring_synergy_table']}}` WHERE run_date = DATE('{{ds}}')",
        use_legacy_sql=False,
        pass_value=False,
        location="EU",
        dag=dag,
    )

    store_historical_conns = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="store_historical_connections_table",
        configuration={
            "query": {
                "query": f"{{% include './bigquery/store_historical.sql' %}}",
                "useLegacySql": "False",
                "destinationTable": {
                    "projectId": "{{ params['storage-prod'] }}",
                    "datasetId": "{{ params['visitor_brand_destinations_dataset'] }}",
                    "tableId": "{{ params['historical_connections_table'] }}",
                },
                "timePartitioning": {"type": "MONTH", "field": "local_date"},
                "clustering": {"fields": ["fk_sgplaces", "fk_sgbrands"]},
                "writeDisposition": "WRITE_APPEND",
                "createDisposition": "CREATE_IF_NEEDED",
                "labels": {"pipeline": "{{ params['pipeline_label_value'] }}"},
            }
        },
        dag=dag,
        location="EU",
    )

    create_postgres = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="create_postgres_table_formatted",
        configuration={
            "query": {
                "query": f"{{% include './bigquery/create_postgres_table_formatted.sql' %}}",
                "useLegacySql": "False",
            },
            "labels": {"pipeline": "{{ params['pipeline_label_value'] }}"},
        },
        dag=dag,
        location="EU",
    )

    create_postgres_percentages = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="create_postgres_percentages",
        configuration={
            "query": {
                "query": f"{{% include './bigquery/create_postgres_percentages.sql' %}}",
                "useLegacySql": "False",
            },
            "labels": {"pipeline": "{{ params['pipeline_label_value'] }}"},
        },
        dag=dag,
        location="EU",
    )

    start >> observed_conns_last_month

    observed_conns_last_month >> observed_conns_last_year

    observed_conns_last_year >> monitoring_observed_conns_query

    monitoring_observed_conns_query >> monitoring_observed_conns_check

    monitoring_observed_conns_check >> monitoring_total_devices_check

    monitoring_total_devices_check >> monitoring_leakage_query

    monitoring_leakage_query >> monitoring_leakage_check

    monitoring_total_devices_check >> monitoring_synergy_query

    monitoring_synergy_query >> monitoring_synergy_check

    [monitoring_leakage_check, monitoring_synergy_check] >> store_historical_conns

    store_historical_conns >> create_postgres

    create_postgres >> create_postgres_percentages
