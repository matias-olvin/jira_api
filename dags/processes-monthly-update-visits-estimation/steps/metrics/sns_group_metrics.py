import os
from importlib.machinery import SourceFileLoader

from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

metrics_path = f"{os.path.dirname(os.path.realpath(__file__))}"
timeseries_groups = SourceFileLoader(
    "timeseries_groups", f"{metrics_path}/timeseries_groups.py"
).load_module()
gtvm_groups = SourceFileLoader(
    "gtvm_groups", f"{metrics_path}/gtvm_groups.py"
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
    update_groups_start = DummyOperator(task_id="update_groups_start")
    start >> update_groups_start

    update_groups_end = DummyOperator(task_id="update_groups_end")

    clear_groups = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="clear_groups",
        configuration={
            "query": {
                "query": "{% include './bigquery/metrics/clear_groups.sql' %}",
                "useLegacySql": "false",
            }
        },
    )
    update_groups_start >> clear_groups

    correlation_groups_end = timeseries_groups.register(
        dag=dag,
        start=clear_groups,
        metric="correlation",
        group_options=["fk_sgbrands", "top_category", "region"],
        granularity_options=["daily", "weekly", "monthly"],
    )

    yoy_error_groups_end = timeseries_groups.register(
        dag=dag,
        start=correlation_groups_end,
        metric="yoy_error",
        group_options=["fk_sgbrands", "top_category", "region"],
        granularity_options=["monthly"],
    )

    divergence_groups_end = gtvm_groups.register(
        dag=dag, start=yoy_error_groups_end, metric="divergence"
    )

    kendall_groups_end = gtvm_groups.register(
        dag=dag, start=divergence_groups_end, metric="kendall"
    )

    discrepancy_groups_end = gtvm_groups.register(
        dag=dag, start=kendall_groups_end, metric="discrepancy"
    )
    discrepancy_groups_end >> update_groups_end

    return update_groups_end
