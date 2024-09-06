"""
DAG ID: postgres_metrics
"""
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


def register(dag, start):  # sourcery skip: use-itertools-product
    """
    Register tasks on the dag.

    Args:
        dag (airflow.models.DAG): DAG instance to register tasks on.
        start (airflow.models.TaskInstance): Task instance all tasks will
            registered downstream from.

    Returns:
        airflow.models.TaskInstance: The last task node in this section.
    """

    clear_gtvm_visits_accuracy = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="clear_gtvm_visits_accuracy",
        configuration={
            "query": {
                "query": "{% include './bigquery/clear_gtvm_visits_accuracy.sql' %}",
                "useLegacySql": "false",
            }
        },
    )
    start >> clear_gtvm_visits_accuracy

    update_gtvm_metric_end = DummyOperator(
        task_id="update_gtvm_metric_end",
    )

    metric_options = ["kendall", "discrepancy", "divergence"]

    for metric in metric_options:
        if metric == "discrepancy":
            query = "{% include './bigquery/update_smc_gtvm_metric_discrepancy.sql' %}"
        else:
            query = "{% include './bigquery/update_smc_gtvm_metric.sql' %}"

        update_gtvm_metric = BigQueryInsertJobOperator(
            task_id=f"update_gtvm_{metric}",
            configuration={
                "query": {
                    "query": "{% include './bigquery/get_name_temp_function.sql' %}"
                    f"{query}",
                    "useLegacySql": "false",
                }
            },
            params={
                "metric": metric,
            },
        )
        clear_gtvm_visits_accuracy >> update_gtvm_metric >> update_gtvm_metric_end

    return update_gtvm_metric_end
