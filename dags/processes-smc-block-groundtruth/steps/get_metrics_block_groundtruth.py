from airflow.models import DAG, TaskInstance
from airflow.utils.task_group import TaskGroup
from common.operators.bigquery import OlvinBigQueryOperator
from common.operators.exceptions import MarkSuccessOperator
from common.utils import callbacks, slack_users


def register(start: TaskInstance, dag: DAG) -> TaskGroup:
    with TaskGroup(group_id="get_metrics_block_groundtruth_task_group") as group:

        query_grid_volume_vis_block_groundtruth = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="query_grid_volume_vis_block_groundtruth",
            query="{% include './include/bigquery/metrics/grid_volume_vis_block_groundtruth.sql' %}",
        )

        query_grid_volume_corr_block_groundtruth = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="query_grid_volume_corr_block_groundtruth",
            query="{% include './include/bigquery/metrics/grid_volume_corr.sql' %}",
        )

        (
            start
            >> query_grid_volume_vis_block_groundtruth
            >> query_grid_volume_corr_block_groundtruth
        )

        query_standalone_vs_child_vis_block_groundtruth = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="query_standalone_vs_child_vis_block_groundtruth",
            query="{% include './include/bigquery/metrics/standalone_vs_child_vis_block_groundtruth.sql' %}",
        )

        query_standalone_vs_child_metrics_block_groundtruth = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="query_standalone_vs_child_metrics_block_groundtruth",
            query="{% include './include/bigquery/metrics/standalone_vs_child_metrics.sql' %}",
        )

        (
            start
            >> query_standalone_vs_child_vis_block_groundtruth
            >> query_standalone_vs_child_metrics_block_groundtruth
        )

        query_visits_volume_vis_block_groundtruth = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="query_visits_volume_vis_block_groundtruth",
            query="{% include './include/bigquery/metrics/visits_volume_vis_block_groundtruth.sql' %}",
            billing_tier="med",
        )

        (start >> query_visits_volume_vis_block_groundtruth)

        query_time_series_analysis_vis_block_groundtruth = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="query_time_series_analysis_vis_block_groundtruth",
            query="{% include './include/bigquery/metrics/time_series_analysis_vis_block_groundtruth.sql' %}",
            billing_tier="high"
            
        )

        (start >> query_time_series_analysis_vis_block_groundtruth)

        metrics_block_groundtruth_end = MarkSuccessOperator(
            task_id="metrics_block_groundtruth_end",
            on_success_callback=callbacks.task_end_custom_slack_alert(
                slack_users.MATIAS, slack_users.IGNACIO, msg_title="*Groundtruth* metrics created."
            ),
        )

        [
            query_grid_volume_corr_block_groundtruth,
            query_standalone_vs_child_metrics_block_groundtruth,
            query_visits_volume_vis_block_groundtruth,
            query_time_series_analysis_vis_block_groundtruth,
        ] >> metrics_block_groundtruth_end

    return group
