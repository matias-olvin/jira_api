from datetime import datetime, timedelta

from airflow.models import DAG, TaskInstance
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
from common.operators.bigquery import OlvinBigQueryOperator, SNSBigQueryOperator
from common.operators.exceptions import MarkSuccessOperator


def register(start: TaskInstance, dag: DAG) -> TaskGroup:
    """Register tasks on the dag.

    Args:
        dag (airflow.models.DAG): DAG instance to register tasks on.
        start (airflow.models.TaskInstance): Task instance all tasks will
            registered downstream from.

    Returns:
        airflow.models.TaskInstance: The last task node in this section.
    """

    def exec_delta_fn(execution_date):
        """
        > It takes the execution date and returns the execution date minus the difference between the
        execution date and the execution date with the time set to 00:00:01

        :param execution_date: The execution date of the task instance
        :return: The execution date minus the difference in seconds between the source and target
        execution dates.
        """
        source_exec_date = execution_date.replace(tzinfo=None)

        target_exec_date = source_exec_date.strftime("%Y-%m-%dT00:00:00")
        target_exec_date = datetime.strptime(f"{target_exec_date}", "%Y-%m-%dT%H:%M:%S")

        diff = source_exec_date - target_exec_date
        diff_seconds = diff.total_seconds()

        return execution_date - timedelta(seconds=diff_seconds)

    with TaskGroup(group_id="poi_feature_processing_task_group") as group:

        create_gtvm_target_agg_sns = SNSBigQueryOperator(
            task_id="create_gtvm_target_agg_sns",
            query="{% include './include/bigquery/groundtruth_volume_visits/create_gtvm_target_agg_sns.sql' %}",
        )

        query_category_median_groundtruth_visits = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="query_category_median_groundtruth_visits",
            query="{% include './include/bigquery/groundtruth_volume_visits/query_category_median_groundtruth_visits.sql' %}",
        )

        trigger_dag_run = TriggerDagRunOperator(
            task_id="trigger_processes-smc-update-prior-visits",
            trigger_dag_id="{{ params['dag-processes-smc-update-prior-visits'] }}",
            execution_date="{{ ds }}",
            reset_dag_run=True,
        )

        dag_run_sensor = ExternalTaskSensor(
            task_id="processes-smc-update-prior-visits_sensor",
            external_dag_id="{{ params['dag-processes-smc-update-prior-visits'] }}",  # same as above
            external_task_id="end",
            external_task_ids=None,  # Use to wait for specifc tasks.
            external_task_group_id=None,  # Use to wait for specifc task group.
            execution_date_fn=exec_delta_fn,
            check_existence=True,  # Check DAG exists.
            mode="reschedule",
            poke_interval=60
            * 2,  # choose appropriate time to poke, if dag runs for 10 minutes, poke every 5 for exmaple
        )

        query_create_poi_features_1 = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="query_create_poi_features_1",
            query="{% include './include/bigquery/groundtruth_volume_visits/create_poi_features_1.sql' %}",
            billing_tier="med",
        )

        query_create_poi_features_2 = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="query_create_poi_features_2",
            query="{% include './include/bigquery/groundtruth_volume_visits/create_poi_features_2.sql' %}",
            billing_tier="higher",
        )

        mark_success_dataforseo = MarkSuccessOperator(
            task_id="mark_success_dataforseo", dag=dag
        )

        query_create_poi_features_v3 = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="query_create_poi_features_v3",
            query="{% include './include/bigquery/groundtruth_volume_visits/create_poi_features_v3.sql' %}",
        )

        query_create_poi_features_visits = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="query_create_poi_features_visits",
            query="{% include './include/bigquery/groundtruth_volume_visits/create_poi_features_visits.sql' %}",
            billing_tier="high",
        )

        query_create_stats_to_unscale = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="query_create_stats_to_unscale",
            query="{% include './include/bigquery/groundtruth_volume_visits/create_stats_to_unscale.sql' %}",
            billing_tier="med",
        )

        (
            start
            >> create_gtvm_target_agg_sns
            >> query_category_median_groundtruth_visits
            >> trigger_dag_run
            >> dag_run_sensor
            >> query_create_poi_features_v3
        )

        (
            query_category_median_groundtruth_visits
            >> query_create_poi_features_1
            >> query_create_poi_features_2
            >> mark_success_dataforseo
            >> query_create_poi_features_v3
        )

        (
            query_create_poi_features_v3
            >> query_create_poi_features_visits
            >> query_create_stats_to_unscale
        )

    return group
