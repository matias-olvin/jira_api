from common.utils import callbacks, slack_users
from airflow.models import TaskInstance, DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from common.operators.exceptions import MarkSuccessOperator
from common.operators.bigquery import OlvinBigQueryOperator

from airflow.utils.task_group import TaskGroup


def register(start: TaskInstance, dag: DAG) -> TaskGroup:

    with TaskGroup(group_id="get_metrics_block_daily_estimation_task_group") as group:

        query_grid_volume_vis_block_daily_estimation = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="query_grid_volume_vis_block_daily_estimation",
            query="{% include './include/bigquery/metrics/grid_volume_vis_block_daily_estimation.sql' %}",
        )

        (
            start
            >> query_grid_volume_vis_block_daily_estimation
        )

        query_grid_volume_corr_block_daily_estimation = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="query_grid_volume_corr_block_daily_estimation",
            query="{% include './include/bigquery/metrics/grid_volume_corr.sql' %}",
        )
        (
            query_grid_volume_vis_block_daily_estimation
            >> query_grid_volume_corr_block_daily_estimation
        )

        query_standalone_vs_child_vis_block_daily_estimation = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="query_standalone_vs_child_vis_block_daily_estimation",
            query="{% include './include/bigquery/metrics/standalone_vs_child_vis_block_daily_estimation.sql' %}",
        )
        (
            start
            >> query_standalone_vs_child_vis_block_daily_estimation
        )

        query_standalone_vs_child_metrics_block_daily_estimation = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="query_standalone_vs_child_metrics_block_daily_estimation",
            query="{% include './include/bigquery/metrics/standalone_vs_child_metrics.sql' %}"
        )
        (
            query_standalone_vs_child_vis_block_daily_estimation
            >> query_standalone_vs_child_metrics_block_daily_estimation
        )

        query_visits_volume_vis_block_daily_estimation = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="query_visits_volume_vis_block_daily_estimation",
            query="{% include './include/bigquery/metrics/visits_volume_vis_block_daily_estimation.sql' %}",
            billing_tier="med",
        )
        (
            start
            >> query_visits_volume_vis_block_daily_estimation
        )

        query_time_series_analysis_vis_block_daily_estimation = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="query_time_series_analysis_vis_block_daily_estimation",
            query="{% include './include/bigquery/metrics/time_series_analysis_vis_block_daily_estimation.sql' %}",
            billing_tier="high",
        )

        start >> query_time_series_analysis_vis_block_daily_estimation

        metrics_block_daily_estimation_end = MarkSuccessOperator(
            task_id="metrics_block_daily_estimation_end",
            on_success_callback=callbacks.task_end_custom_slack_alert(
                slack_users.MATIAS,
                slack_users.IGNACIO,
                msg_title="*Daily estimation* metrics created.",
            ),
        )

        [
            query_grid_volume_corr_block_daily_estimation,
            query_standalone_vs_child_metrics_block_daily_estimation,
            query_visits_volume_vis_block_daily_estimation,
            query_time_series_analysis_vis_block_daily_estimation,
        ] >> metrics_block_daily_estimation_end

    return group
