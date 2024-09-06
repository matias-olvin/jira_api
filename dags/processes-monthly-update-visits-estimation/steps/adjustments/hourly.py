"""
DAG ID: real_visits
"""
from datetime import datetime

from airflow.models import Variable
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.dummy_operator import DummyOperator
from common.utils.callbacks import check_dashboard_slack_alert
from common.operators.bigquery import OlvinBigQueryOperator
from common.operators.exceptions import MarkSuccessOperator
from common.operators.validations import TrendValidationOperator
from common.processes.validations import visits_estimation_validation


def register(dag, start, env):
    """Register tasks on the dag.

    Args:
        dag (airflow.models.DAG): DAG instance to register tasks on.
        start (airflow.models.TaskInstance): Task instance all tasks will
            registered downstream from.

    Returns:
        airflow.models.TaskInstance: The last task node in this section.

    """
    # dummy_hourly = DummyOperator(task_id="dummy_hourly", dag=dag)

    geo_visits_poi = OlvinBigQueryOperator(
task_id="geo_visits_poi",
        query="{% include './bigquery/adjustments/hourly/geo_visits_poi.sql' %}",
        dag=dag,
        billing_tier="med",
    )

    geo_visits_grouped = OlvinBigQueryOperator(
task_id="geo_visits_grouped",
        query="{% include './bigquery/adjustments/hourly/geo_visits_grouped.sql' %}",
        dag=dag,
        billing_tier="med",
    )

    hourly_output = OlvinBigQueryOperator(
task_id="hourly_output",
        query="{% include './bigquery/adjustments/hourly/hourly_output.sql' %}",
        dag=dag,
        billing_tier="high",
    )

    create_dashboard_opening_hours = OlvinBigQueryOperator(
        task_id="create_dashboard_opening_hours",
        query="{% include './bigquery/metrics/brand_agg/hourly_summary.sql' %}",
        billing_tier="med",
    )

    mark_success_hourly = MarkSuccessOperator(task_id="mark_success_hourly", dag=dag)

    (
        start
        >> [geo_visits_poi, geo_visits_grouped]
        >> hourly_output
        >> create_dashboard_opening_hours
        >> mark_success_hourly
    )

    step = "adjustments_hourly"

    validations_start = DummyOperator(task_id="hourly_trend_validation_start")
    mark_success_hourly >> validations_start

    migrate_hourly_to_accessible_by_sns = OlvinBigQueryOperator(
task_id="migrate_hourly_to_accessible_by_sns",
        query=f"CREATE OR REPLACE TABLE `{Variable.get('env_project')}.accessible_by_sns.visits_estimation-adjustments_output_hourly` COPY `{Variable.get('env_project')}.visits_estimation.adjustments_output_hourly`;",
    )

    trend_hourly_validation = TrendValidationOperator(
        task_id=f"{step}_trend_hourly",
        sub_validation="group",
        env=env,
        pipeline="visits_estimation",
        step=step,
        run_date="{{ ds }}",
        source=f"{Variable.get('env_project')}.accessible_by_sns.visits_estimation-adjustments_output_hourly",
        destination=f"sns-vendor-olvin-poc.{Variable.get('accessible_by_olvin')}.visits_estimation_metrics-ground_truth_trend",
        granularity="hour",
        date_ranges=[["2022-06-01", "2022-06-14"]],
        classes=["fk_sgbrands", "naics_code", "region", "top_category"],
    )

    migrate_hourly_results_to_trend_table = OlvinBigQueryOperator(
task_id="migrate_hourly_results_to_trend_table",
        query=f"DELETE FROM `{Variable.get('env_project')}.{dag.params['metrics_dataset']}.ground_truth_trend` WHERE step = '{step}' AND pipeline = 'visits_estimation' and granularity = 'hour'; \n \
                INSERT INTO `{Variable.get('env_project')}.{dag.params['metrics_dataset']}.ground_truth_trend` SELECT * FROM `sns-vendor-olvin-poc.{Variable.get('accessible_by_olvin')}.visits_estimation_metrics-ground_truth_trend`;",
    )

    (
        validations_start
        >> migrate_hourly_to_accessible_by_sns
        >> trend_hourly_validation
        >> migrate_hourly_results_to_trend_table
    )

    hourly_validation = visits_estimation_validation(
        input_table=f"{Variable.get('env_project')}.{dag.params['visits_estimation_dataset']}.{dag.params['adjustments_output_table']}",
        env=env,
        pipeline="visits_estimation",
        step=step,
        run_date="{{ ds }}",
        classes=["fk_sgbrands", "naics_code", "region", "top_category"],
        start_task=validations_start,
    )

    trigger_visualization_pipeline = TriggerDagRunOperator(
        task_id=f"trigger_visualization_pipeline_{step}",
        trigger_dag_id=f"{{{{ dag.dag_id }}}}"+'-visualizations',
        execution_date=f"{{{{ ds }}}} {datetime.now().time()}",
        conf={
            "step": f"{step}",
            "source_table": f"{Variable.get('env_project')}.{dag.params['visits_estimation_dataset']}.{dag.params['adjustments_output_table']}",
        },
        wait_for_completion=True,
        dag=dag,
    )

    mark_success_hourly_validation = MarkSuccessOperator(
        task_id=f"mark_success_{step}_validation",
        on_failure_callback=check_dashboard_slack_alert(
            "U03BANPLXJR",
            message=f"Check metrics on {step}. Also check hours make sense",
        ),
        dag=dag,
    )
    (
        mark_success_hourly
        >> trigger_visualization_pipeline
        >> mark_success_hourly_validation
    )
    [
        migrate_hourly_results_to_trend_table,
        hourly_validation,
    ] >> mark_success_hourly_validation

    return mark_success_hourly_validation
