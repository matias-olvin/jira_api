from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


def register(dag, start, metric):  # sourcery skip: use-itertools-product
    """
    Register tasks on the dag.

    Args:
        dag (airflow.models.DAG): DAG instance to register tasks on.
        start (airflow.models.TaskInstance): Task instance all tasks will
            registered downstream from.

    Returns:
        airflow.models.TaskInstance: The last task node in this section.
    """
    metric_start = DummyOperator(task_id=f"update_groups_start_{metric}")
    start >> metric_start

    metric_end = DummyOperator(task_id=f"update_groups_end_{metric}")

    if metric == "discrepancy":
        query = "{% include './bigquery/metrics/update_groups_gtvm_discrepancy.sql' %}"
    else:
        query = "{% include './bigquery/metrics/update_groups_gtvm.sql' %}"

    update_groups = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id=f"update_groups_{metric}",
        configuration={
            "query": {
                "query": "{% include './bigquery/metrics/get_name_temp_function.sql' %} "
                f"{query} ",
                "useLegacySql": "false",
            }
        },
        params={
            "metric": metric,
        },
    )
    metric_start >> update_groups >> metric_end

    return metric_end
