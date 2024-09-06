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

DAG_ID = path.split("/")[-1]

env_args = dag_args.make_env_args(
    dag_id=DAG_ID,
)
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

    preproces_market = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="create_preprocess_market_table",
        configuration={
            "query": {
                "query": f"{{% include './bigquery/preprocess_market_analysis.sql' %}}",
                "useLegacySql": "False",
            },
            "labels": {"pipeline": "{{ params['pipeline_label_value'] }}"},
        },
        dag=dag,
        location="EU",
    )

    monitoring_observed_conns_query = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="monitoring_table_size_query",
        configuration={
            "query": {
                "query": f"{{% include './bigquery/tests/check_size.sql' %}}",
                "useLegacySql": "False",
            },
            "labels": {"pipeline": "{{ params['pipeline_label_value'] }}"},
        },
        dag=dag,
        location="EU",
    )

    monitoring_observed_conns_check = BigQueryValueCheckOperator(
gcp_conn_id="google_cloud_olvin_default",
        task_id="monitoring_table_size_check",
        retries=1,
        sql="SELECT terminate FROM `{{params['storage-prod']}}.{{params['visitor_brand_destinations_metrics_dataset']}}.{{params['monitoring_market_table']}}` WHERE month_update = FORMAT_DATE('%B', DATE_TRUNC(DATE_SUB( DATE('{{ds}}'), INTERVAL 0 MONTH), MONTH)  )",
        use_legacy_sql=False,
        pass_value=False,
        location="EU",
        dag=dag,
    )

    start >> preproces_market

    preproces_market >> monitoring_observed_conns_query

    monitoring_observed_conns_query >> monitoring_observed_conns_check
