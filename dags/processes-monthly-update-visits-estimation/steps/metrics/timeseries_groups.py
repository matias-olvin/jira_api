from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


def register(
    dag, start, metric, group_options, granularity_options
):  # sourcery skip: use-itertools-product
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

    if metric == "yoy_error":
        query = (
            "{% include './bigquery/metrics/update_groups_timeseries_yoy_error.sql' %}"
        )
    else:
        query = "{% include './bigquery/metrics/update_groups_timeseries.sql' %}"

    prev_task = metric_start
    for granularity in granularity_options:
        granularity_start = DummyOperator(
            task_id=f"update_groups_start_{metric}_{granularity}"
        )
        prev_task >> granularity_start

        granularity_end = DummyOperator(
            task_id=f"update_groups_end_{metric}_{granularity}"
        )

        prev_task = granularity_start
        for group in group_options:
            update_groups = BigQueryInsertJobOperator(
                task_id=f"update_groups_{metric}_{granularity}_{group}",
                configuration={
                    "query": {
                        "query": "{% include './bigquery/metrics/get_name_temp_function.sql' %} "
                        f"{query} ",
                        "useLegacySql": "false",
                    }
                },
                params={
                    "granularity": granularity,
                    "group_id": group,
                    "metric": metric,
                },
            )
            prev_task >> update_groups
            prev_task = update_groups

        prev_task >> granularity_end
        prev_task = granularity_end

    prev_task >> metric_end

    return metric_end
