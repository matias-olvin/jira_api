from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


def register(
    dag, start, metric, granularity_options
):  # sourcery skip: use-itertools-product
    """
    Register tasks on the dag.

    Args:
        dag (airflow.models.DAG): DAG instance to register tasks on.
        start (airflow.models.TaskInstance): Task instance all tasks will
            registered downstream from.
        metric (str): metric to be added to table.
        granularity_options (list[str]): List of strings for the granularities
            to get the metric for.

    Returns:
        airflow.models.TaskInstance: The last task node in this section.
    """
    metric_start = DummyOperator(task_id=f"update_pois_start_{metric}")
    start >> metric_start

    metric_end = DummyOperator(task_id=f"update_pois_end_{metric}")

    if metric == "yoy_error":
        query = "{% include './bigquery/metrics/update_pois_yoy_error.sql' %}"
    else:
        query = "{% include './bigquery/metrics/update_pois.sql' %}"

    prev_task = metric_start
    for granularity in granularity_options:
        update_pois = BigQueryInsertJobOperator(
            task_id=f"update_pois_{metric}_{granularity}",
            configuration={"query": {"query": f"{query}", "useLegacySql": "false"}},
            params={
                "granularity": granularity,
                "metric": metric,
            },
        )
        prev_task >> update_pois
        prev_task = update_pois

    prev_task >> metric_end

    return metric_end
