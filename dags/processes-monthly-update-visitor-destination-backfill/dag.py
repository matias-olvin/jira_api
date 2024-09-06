"""
DAG ID: sg_agg_stats
"""
import os, pendulum
from datetime import date, datetime, timedelta
from importlib.machinery import SourceFileLoader
from common.utils import dag_args, callbacks
import pandas as pd

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

DAG_ID = path.split("/")[-1]

env_args = dag_args.make_env_args(
    dag_id=DAG_ID,
)


send_2_posgres = SourceFileLoader(
    "send_2_posgres", steps_path + "send_2_postgres.py"
).load_module()


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
    tags=[env_args["env"], "monthly", "backfill"],
    params=dag_args.load_config(__file__),
    # doc_md=dag_args.load_docs(__file__),
    user_defined_macros={},
) as dag:
    start = DummyOperator(task_id="start", dag=dag)

    prev_task = start

    dates_compute = pd.date_range(
        "2019-01-01", date.today().strftime("%Y-%m-%d"), freq=f"{1}MS", inclusive="left"
    )
    for timestamp_start in dates_compute[:-1]:
        date_start = timestamp_start.strftime("%Y-%m-%d")
        year_start = timestamp_start.strftime("%Y")

        observed_conns_last_month = BigQueryInsertJobOperator(
            task_id=f"create_observed_connections_table_{date_start}",
            configuration={
                "query": {
                    "query": f"{{% with "
                    f"md_date_start='{date_start}'"
                    f"%}}{{% include './bigquery/create_observed_connections_table.sql' %}}{{% endwith %}}",
                    "useLegacySql": "False",
                    "destinationTable": {
                        "projectId": "{{ params['storage-prod'] }}",
                        "datasetId": "{{ params['visitor_brand_destinations_monthly_dataset'] }}",
                        "tableId": year_start,
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
            task_id=f"aggregate_observed_connections_last_year_table_{date_start}",
            configuration={
                "query": {
                    "query": f"{{% with "
                    f"md_date_start='{date_start}'"
                    f"%}}{{% include './bigquery/aggregate_last_12_months.sql' %}}{{% endwith %}}",
                    "useLegacySql": "False",
                },
                "labels": {"pipeline": "{{ params['pipeline_label_value'] }}"},
            },
            dag=dag,
            location="EU",
        )

        monitoring_observed_conns_query = BigQueryInsertJobOperator(
            task_id=f"monitoring_observed_conns_query_{date_start}",
            configuration={
                "query": {
                    "query": f"{{% with "
                    f"md_date_start='{date_start}'"
                    f"%}}{{% include './bigquery/tests/observed_connections.sql' %}}{{% endwith %}}",
                    "useLegacySql": "False",
                },
                "labels": {"pipeline": "{{ params['pipeline_label_value'] }}"},
            },
            dag=dag,
            location="EU",
        )

        monitoring_leakage_query = BigQueryInsertJobOperator(
            task_id=f"monitoring_leakage_query_{date_start}",
            configuration={
                "query": {
                    "query": f"{{% with "
                    f"md_date_start='{date_start}'"
                    f"%}}{{% include './bigquery/metrics/test_leakage_ferber.sql' %}}{{% endwith %}}",
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

        monitoring_synergy_query = BigQueryInsertJobOperator(
            task_id=f"monitoring_synergy_query_{date_start}",
            configuration={
                "query": {
                    "query": f"{{% with "
                    f"md_date_start='{date_start}'"
                    f"%}}{{% include './bigquery/metrics/test_synergy_ferber.sql' %}}{{% endwith %}}",
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

        store_historical_conns = BigQueryInsertJobOperator(
            task_id=f"store_historical_connections_table_{date_start}",
            configuration={
                "query": {
                    "query": f"{{% with "
                    f"md_date_start='{date_start}'"
                    f"%}}{{% include './bigquery/store_historical.sql' %}}{{% endwith %}}",
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

        # create_postgres = BigQueryInsertJobOperator(
        #     task_id=f"create_postgres_table_formatted_{date_start}",
        #     configuration={
        #         "query": {
        #             "query": f"{{% with "
        #                      f"md_date_start='{date_start}'"
        #                      f"%}}{{% include './bigquery/create_postgres_table_formatted.sql' %}}{{% endwith %}}",
        #             "useLegacySql": "False",
        #         },
        #         "labels": {
        #             "pipeline": "{{ params['pipeline_label_value'] }}"
        #         }
        #     },
        #     dag=dag,
        #     location="EU",
        # )

        prev_task >> observed_conns_last_month

        observed_conns_last_month >> observed_conns_last_year

        observed_conns_last_year >> monitoring_observed_conns_query

        monitoring_observed_conns_query >> monitoring_leakage_query

        monitoring_observed_conns_query >> monitoring_synergy_query

        [monitoring_leakage_query, monitoring_synergy_query] >> store_historical_conns

        prev_task = store_historical_conns
