"""
DAG ID: real_visits
"""
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from common.utils.callbacks import check_dashboard_slack_alert
from common.operators.bigquery import OlvinBigQueryOperator
from common.operators.exceptions import MarkSuccessOperator
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
    query_covid_restrictions = OlvinBigQueryOperator(
task_id="query_covid_restrictions",
        query="{% include './bigquery/adjustments/covid/covid.sql' %}",
        dag=dag,
        billing_tier="med",
    )
    start >> query_covid_restrictions

    create_ts_brands_covid = OlvinBigQueryOperator(
        task_id="create_ts_brands_covid",
        query="{% include './bigquery/metrics/brand_agg/covid.sql' %}",
        billing_tier="med",
    )
    query_covid_restrictions >> create_ts_brands_covid

    mark_success_covid = MarkSuccessOperator(task_id="mark_success_covid", dag=dag)
    create_ts_brands_covid >> mark_success_covid

    step = "adjustments_covid"

    validations_start = DummyOperator(task_id=f"{step}_validation_start")
    mark_success_covid >> validations_start

    covid_validation = visits_estimation_validation(
        input_table=f"{Variable.get('env_project')}.{dag.params['visits_estimation_dataset']}.{dag.params['adjustments_covid_output_table']}",
        env=env,
        pipeline="visits_estimation",
        step=step,
        run_date="{{ ds }}",
        classes=["fk_sgbrands", "naics_code", "region", "top_category"],
        start_task=validations_start,
    )

    mark_success_covid_validation = MarkSuccessOperator(
        task_id=f"mark_success_{step}_validation",
        on_failure_callback=check_dashboard_slack_alert(
            "U03BANPLXJR", message=f"Check metrics on {step}."
        ),
        dag=dag,
    )

    covid_validation >> mark_success_covid_validation

    validations_end = DummyOperator(task_id=f"{step}_validation_end")
    mark_success_covid_validation >> validations_end

    return mark_success_covid_validation
