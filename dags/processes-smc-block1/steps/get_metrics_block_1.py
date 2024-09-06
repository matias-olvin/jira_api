from common.utils import callbacks, slack_users
from airflow.models import TaskInstance, DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator

from common.operators.bigquery import OlvinBigQueryOperator
from common.operators.exceptions import MarkSuccessOperator


def register(start: TaskInstance, dag: DAG) -> TaskGroup:
    with TaskGroup(group_id="get_metrics_block_1_task_group") as group:

        query_grid_volume_vis_block_1 = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="query_grid_volume_vis_block_1",
            query="{% include './include/bigquery/metrics/grid_volume_vis_block_1.sql' %}",
        )

        start >> query_grid_volume_vis_block_1

        query_grid_volume_corr_block_1 = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="query_grid_volume_corr_block_1",
            query="{% include './include/bigquery/metrics/grid_volume_corr.sql' %}",
        )

        query_grid_volume_vis_block_1 >> query_grid_volume_corr_block_1

        query_standalone_vs_child_vis_block_1 = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="query_standalone_vs_child_vis_block_1",
            query="{% include './include/bigquery/metrics/standalone_vs_child_vis_block_1.sql' %}",
        )
        start >> query_standalone_vs_child_vis_block_1

        query_standalone_vs_child_metrics_block_1 = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="query_standalone_vs_child_metrics_block_1",
            query="{% include './include/bigquery/metrics/standalone_vs_child_metrics.sql' %}",
        )

        query_standalone_vs_child_vis_block_1 >> query_standalone_vs_child_metrics_block_1

        query_visits_volume_vis_block_1 = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="query_visits_volume_vis_block_1",
            query="{% include './include/bigquery/metrics/visits_volume_vis_block_1.sql' %}",
            billing_tier="med",
        )
        start >> query_visits_volume_vis_block_1

        query_time_series_analysis_vis_block_1 = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="query_time_series_analysis_vis_block_1",
            query="{% include './include/bigquery/metrics/time_series_analysis_vis.sql' %}",
            billing_tier="high",
        )

        start >> query_time_series_analysis_vis_block_1

        query_weekly_categories_distribution_block_1 = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="query_weekly_categories_distribution_block_1",
            query="{% include './include/bigquery/metrics/weekly_categories_distribution.sql' %}",
            billing_tier="med",
        )

        start >> query_weekly_categories_distribution_block_1

        metrics_block_1_end = MarkSuccessOperator(
            task_id="metrics_block_1_end",
            on_success_callback=callbacks.task_end_custom_slack_alert(
                slack_users.MATIAS, slack_users.IGNACIO, msg_title="*Block 1* metrics created."
            ),
        ) 


        [
            query_grid_volume_corr_block_1,
            query_standalone_vs_child_metrics_block_1,
            query_visits_volume_vis_block_1,
            query_time_series_analysis_vis_block_1,
            query_weekly_categories_distribution_block_1,
        ] >> metrics_block_1_end

    return group
