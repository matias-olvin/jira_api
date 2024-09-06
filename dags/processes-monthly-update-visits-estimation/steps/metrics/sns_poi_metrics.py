import os
from importlib.machinery import SourceFileLoader

from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

metrics_path = f"{os.path.dirname(os.path.realpath(__file__))}"
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
    update_pois_start = DummyOperator(task_id="update_pois_start")
    start >> update_pois_start

    update_pois_end = DummyOperator(task_id="update_pois_end")

    clear_pois = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="clear_pois",
        configuration={
            "query": {
                "query": "{% include './bigquery/metrics/clear_pois.sql' %}",
                "useLegacySql": "false",
            }
        },
    )
    update_pois_start >> clear_pois

    correlation_pois_end = timeseries_pois.register(
        dag=dag,
        start=clear_pois,
        metric="correlation",
        granularity_options=["daily", "weekly", "monthly"],
    )

    yoy_error_pois_end = timeseries_pois.register(
        dag=dag,
        start=correlation_pois_end,
        metric="yoy_error",
        granularity_options=["monthly"],
    )
    yoy_error_pois_end >> update_pois_end

    return update_pois_end
