"""
DAG ID: real_visits
"""
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from common.operators.bigquery import OlvinBigQueryOperator
from common.operators.exceptions import MarkSuccessOperator
from common.processes.validations import visits_estimation_validation
from common.utils.callbacks import check_dashboard_slack_alert


def register(dag, start, env):
    """Register tasks on the dag.

    Args:
        dag (airflow.models.DAG): DAG instance to register tasks on.
        start (airflow.models.TaskInstance): Task instance all tasks will
            registered downstream from.

    Returns:
        airflow.models.TaskInstance: The last task node in this section.

    """

    places_dynamic_query = BigQueryInsertJobOperator(
        gcp_conn_id="google_cloud_olvin_default",
        task_id="places_dynamic_query",
        project_id=Variable.get("env_project"),
        configuration={
            "query": {
                "query": "{% include './bigquery/adjustments/places_dynamic/places_dynamic.sql' %}",
                "useLegacySql": "False",
                "create_session": "True",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id |  replace('.','-') }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )
    start >> places_dynamic_query

    create_ts_brands_places_dynamic = OlvinBigQueryOperator(
        task_id="create_ts_brands_places_dynamic",
        query="{% include './bigquery/metrics/brand_agg/places_dynamic.sql' %}",
        billing_tier="med",
    )

    create_dashboard_open_pois_per_brand = OlvinBigQueryOperator(
        task_id="create_dashboard_open_pois_per_brand",
        query="{% include './bigquery/metrics/brand_agg/num_openings_places_dynamic.sql' %}",
        billing_tier="med",
    )

    create_dashboard_month_on_month_descent = OlvinBigQueryOperator(
        task_id="create_dashboard_month_on_month_descent",
        query="{% include './bigquery/adjustments/places_dynamic/brand_visits_descent.sql' %}",
        billing_tier="med",
    )

    mark_success_places_dynamic = MarkSuccessOperator(
        task_id="mark_success_places_dynamic", dag=dag
    )

    check_distinct_fk_sgplaces_before_after_transform = OlvinBigQueryOperator(
        task_id="check_distinct_fk_sgplaces_before_after_transform",
        query="{% include './bigquery/adjustments/places_dynamic/check_distinct_fk_sgplaces_before_after_transform.sql' %}",
        billing_tier="med",
    )

    (
        places_dynamic_query
        >> [
            create_ts_brands_places_dynamic,
            create_dashboard_open_pois_per_brand,
            create_dashboard_month_on_month_descent,
        ]
        >> check_distinct_fk_sgplaces_before_after_transform
    )

    step = "adjustments_places_dynamic"

    validations_start = EmptyOperator(task_id=f"{step}_validation_start")

    (
        check_distinct_fk_sgplaces_before_after_transform
        >> mark_success_places_dynamic
        >> validations_start
    )

    places_dynamic_validation = visits_estimation_validation(
        input_table=f"{Variable.get('env_project')}.{dag.params['visits_estimation_dataset']}.{dag.params['adjustments_places_dynamic_output_table']}",
        env=env,
        pipeline="visits_estimation",
        step=step,
        run_date="{{ ds }}",
        classes=["fk_sgbrands", "naics_code", "region", "top_category"],
        start_task=validations_start,
    )

    mark_success_places_dynamic_validation = MarkSuccessOperator(
        task_id=f"mark_success_{step}_validation",
        on_failure_callback=check_dashboard_slack_alert(
            "U03BANPLXJR",
            message=f"Check metrics on {step}. Also check number of rows does not drop drastically",
        ),
        dag=dag,
    )

    places_dynamic_validation >> mark_success_places_dynamic_validation

    return mark_success_places_dynamic_validation
