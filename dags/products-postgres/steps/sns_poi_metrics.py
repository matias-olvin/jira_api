import os
from importlib.machinery import SourceFileLoader

from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

metrics_path = f"{os.path.dirname(os.path.realpath(__file__))}/metrics"
timeseries_pois = SourceFileLoader(
    "timeseries_pois", f"{metrics_path}/timeseries_pois.py"
).load_module()


def register(dag, start):
    """
    Register tasks on the dag.

    Args:
        dag (airflow.models.DAG): DAG instance to register tasks on.
        start (airflow.models.TaskInstance): Task instance all tasks will
            registered downstream from.

    Returns:
        airflow.models.TaskInstance: The last task node in this section.
    """
    update_almanac_pois_start = DummyOperator(task_id="update_almanac_pois_start")
    start >> update_almanac_pois_start

    update_almanac_pois_end = DummyOperator(task_id="update_almanac_pois_end")

    clear_almanac_pois = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="clear_almanac_pois",
        configuration={
            "query": {
                "query": "{% include './bigquery/clear_almanac_pois.sql' %}",
                "useLegacySql": "false",
            }
        },
    )
    update_almanac_pois_start >> clear_almanac_pois

    correlation_pois_end = timeseries_pois.register(
        dag=dag,
        start=clear_almanac_pois,
        metric="correlation",
        granularity_options=["hourly", "daily", "weekly", "monthly"],
    )

    yoy_error_pois_end = timeseries_pois.register(
        dag=dag,
        start=correlation_pois_end,
        metric="yoy_error",
        granularity_options=["monthly"],
    )
    yoy_error_pois_end >> update_almanac_pois_end

    return update_almanac_pois_end
