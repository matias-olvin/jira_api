from airflow.models import DAG, TaskInstance
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from common.operators.bigquery import OlvinBigQueryOperator


def register(start: TaskInstance, dag: DAG):
    with TaskGroup(group_id="get_metrics_block_0_task_group") as group:

        query_grid_volume_vis_block_0 = OlvinBigQueryOperator(
            task_id="query_grid_volume_vis_block_0",
            query="{% include './include/bigquery/block_0/metrics/grid_volume_vis_block_0.sql' %}",
            billing_tier="med",
        )

        start >> query_grid_volume_vis_block_0

        query_grid_volume_corr_block_0 = OlvinBigQueryOperator(
            task_id="query_grid_volume_corr_block_0",
            query="{% include './include/bigquery/block_0/metrics/grid_volume_corr.sql' %}",
        )
        query_grid_volume_vis_block_0 >> query_grid_volume_corr_block_0

        query_standalone_vs_child_vis_block_0 = OlvinBigQueryOperator(
            task_id="query_standalone_vs_child_vis_block_0",
            query="{% include './include/bigquery/block_0/metrics/standalone_vs_child_vis_block_0.sql' %}",
            billing_tier="med",
        )

        start >> query_standalone_vs_child_vis_block_0

        query_standalone_vs_child_metrics_block_0 = OlvinBigQueryOperator(
            task_id="query_standalone_vs_child_metrics_block_0",
            query="{% include './include/bigquery/block_0/metrics/standalone_vs_child_metrics.sql' %}",
        )

        (
            query_standalone_vs_child_vis_block_0
            >> query_standalone_vs_child_metrics_block_0
        )

        query_visits_volume_vis_block_0 = OlvinBigQueryOperator(
            task_id="query_visits_volume_vis_block_0",
            query="{% include './include/bigquery/block_0/metrics/visits_volume_vis_block_0.sql' %}",
            billing_tier="high",
        )

        start >> query_visits_volume_vis_block_0

        query_time_series_analysis_vis_block_0 = OlvinBigQueryOperator(
            task_id="query_time_series_analysis_vis_block_0",
            query="{% include './include/bigquery/block_0/metrics/time_series_analysis_vis.sql' %}",
            billing_tier="highest",
        )
        start >> query_time_series_analysis_vis_block_0

        metrics_block_0_end = EmptyOperator(
            task_id="metrics_block_0_end",
        )

        [
            query_grid_volume_corr_block_0,
            query_standalone_vs_child_metrics_block_0,
            query_visits_volume_vis_block_0,
            query_time_series_analysis_vis_block_0,
        ] >> metrics_block_0_end

    return group
