"""
DAG ID: real_visits
"""
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from common.utils.callbacks import check_dashboard_slack_alert
from common.operators.bigquery import OlvinBigQueryOperator
from common.operators.exceptions import MarkSuccessOperator
from common.processes.triggers import trigger_dag_run
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
    trigger_sns_for_closings = trigger_dag_run(
        start=start,
        env=env,
        trigger_dag_id="visits_estimation_adjustments_closing_hours",
        destination_table=[
            f"{Variable.get('env_project')}.{dag.params['visits_estimation_dataset']}.adjustments_closing_days_with_zero_visits",
        ],
    )

    closings_logic_query = OlvinBigQueryOperator(
task_id="closings_logic_query",
        query="{% include './bigquery/adjustments/closings/get_closing_days.sql' %}",
        dag=dag,
    )
    trigger_sns_for_closings >> closings_logic_query

    closings_output_query = OlvinBigQueryOperator(
task_id="closings_output_query",
        query="{% include './bigquery/adjustments/closings/adjust_visits_for_closing_days.sql' %}",
        dag=dag,
        billing_tier="med",
    )
    closings_logic_query >> closings_output_query

    create_ts_brands_closings = OlvinBigQueryOperator(
        task_id="create_ts_brands_closings",
        query="{% include './bigquery/metrics/brand_agg/closings.sql' %}",
        billing_tier="med",
    )
    closings_output_query >> create_ts_brands_closings

    mark_success_closings_model = MarkSuccessOperator(
        task_id="mark_success_closings_model", dag=dag
    )
    create_ts_brands_closings >> mark_success_closings_model

    step = "adjustments_closing"

    validations_start = DummyOperator(task_id=f"{step}_validation_start")
    mark_success_closings_model >> validations_start

    closings_validation = visits_estimation_validation(
        input_table=f"{Variable.get('env_project')}.{dag.params['visits_estimation_dataset']}.{dag.params['adjustments_closings_output']}",
        env=env,
        pipeline="visits_estimation",
        step=step,
        run_date="{{ ds }}",
        classes=["fk_sgbrands", "naics_code", "region", "top_category"],
        start_task=validations_start,
    )

    mark_success_closings_volume_validation = MarkSuccessOperator(
        task_id=f"mark_success_{step}_validation",
        on_failure_callback=check_dashboard_slack_alert(
            "U03BANPLXJR",
            data_studio_link="https://lookerstudio.google.com/reporting/07196097-8f5b-41d6-a6bf-b1b261bfe89d/page/p_j2j42t5i4c",
            message=f"Check metrics on {step}.",
        ),
        dag=dag,
    )

    closings_validation >> mark_success_closings_volume_validation

    validations_end = DummyOperator(task_id=f"{step}_validation_end")
    mark_success_closings_volume_validation >> validations_end

    return mark_success_closings_volume_validation
