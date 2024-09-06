"""
DAG ID: real_visits
"""

from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryValueCheckOperator
from common.operators.bigquery import (
    OlvinBigQueryOperator,
    SNSBigQueryOperator,
)
from common.operators.exceptions import MarkSuccessOperator
from common.processes.validations import visits_estimation_validation
from common.utils import callbacks
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

    ref_dates_events_query = OlvinBigQueryOperator(
        task_id="ref_dates_events",
        query="{% include './bigquery/adjustments/events/ref_dates_events.sql' %}",
        dag=dag,
    )

    start >> ref_dates_events_query

    dates_interval_check = BigQueryValueCheckOperator(
        gcp_conn_id="google_cloud_olvin_default",
        sql="{% include './bigquery/adjustments/events/tests/ref_dates/right_interval.sql' %}",
        pass_value=0,
        task_id="dates_interval_check",
        dag=dag,
        location="EU",
        use_legacy_sql=False,
        depends_on_past=False,
        wait_for_downstream=False,
        on_failure_callback=callbacks.task_fail_slack_alert(
            "U03BANPLXJR",
            channel=env,  # Carlos
        ),
    )

    pois_in_sns_check = BigQueryValueCheckOperator(
        gcp_conn_id="google_cloud_olvin_default",
        sql="{% include './bigquery/adjustments/events/tests/ref_dates/all_pois_in_sns_list.sql' %}",
        pass_value=0,
        task_id="pois_in_sns_check",
        dag=dag,
        location="EU",
        use_legacy_sql=False,
        depends_on_past=False,
        wait_for_downstream=False,
        on_failure_callback=callbacks.task_fail_slack_alert(
            "U03BANPLXJR",
            channel=env,  # Carlos
        ),
    )

    no_duplicates_ref_dates_check = BigQueryValueCheckOperator(
        gcp_conn_id="google_cloud_olvin_default",
        sql="{% include './bigquery/adjustments/events/tests/ref_dates/no_duplicates.sql' %}",
        pass_value=0,
        task_id="no_duplicates_check",
        dag=dag,
        location="EU",
        use_legacy_sql=False,
        depends_on_past=False,
        wait_for_downstream=False,
        on_failure_callback=callbacks.task_fail_slack_alert(
            "U03BANPLXJR",
            channel=env,  # Carlos
        ),
    )

    move_ref_dates_to_accessible_by_sns = OlvinBigQueryOperator(
        task_id="move_ref_dates_to_accessible_by_sns",
        query="{% include './bigquery/adjustments/events/copy_dates_to_acc_sns.sql' %}",
        dag=dag,
    )

    ref_dates_done = DummyOperator(task_id="ref_dates_done")

    (
        ref_dates_events_query
        >> [dates_interval_check, pois_in_sns_check, no_duplicates_ref_dates_check]
        >> move_ref_dates_to_accessible_by_sns
        >> ref_dates_done
    )

    move_events_to_working = SNSBigQueryOperator(
        task_id="move_events_to_working",
        query="{% include './bigquery/adjustments/events/sns/copy_to_working.sql' %}",
    )
    sns_query_events = SNSBigQueryOperator(
        task_id="sns_events_query",
        query="{% include './bigquery/adjustments/events/sns/events.sql' %}",
    )
    move_events_to_accessible_by_olvin = SNSBigQueryOperator(
        task_id="move_events_to_accessible_by_olvin",
        query="{% include './bigquery/adjustments/events/sns/copy_to_accessible_by_olvin.sql' %}",
    )

    (
        ref_dates_done
        >> move_events_to_working
        >> sns_query_events
        >> move_events_to_accessible_by_olvin
    )

    move_factors_from_acc_olvin = OlvinBigQueryOperator(
        task_id="move_factors_from_acc_olvin",
        query="{% include './bigquery/adjustments/events/copy_factors_from_acc_olvin.sql' %}",
        dag=dag,
    )

    same_tier_ids_check = BigQueryValueCheckOperator(
        gcp_conn_id="google_cloud_olvin_default",
        sql="{% include './bigquery/adjustments/events/tests/factor_per_tier_date/same_tier_ids.sql' %}",
        pass_value=0,
        task_id="same_tier_ids_check",
        dag=dag,
        location="EU",
        use_legacy_sql=False,
        depends_on_past=False,
        wait_for_downstream=False,
        on_failure_callback=callbacks.task_fail_slack_alert(
            "U03BANPLXJR",
            channel=env,  # Carlos
        ),
    )

    no_nulls_tier_factor_check = BigQueryValueCheckOperator(
        gcp_conn_id="google_cloud_olvin_default",
        sql="{% include './bigquery/adjustments/events/tests/factor_per_tier_date/null_factors.sql' %}",
        pass_value=0,
        task_id="no_nulls_tier_factor_check",
        dag=dag,
        location="EU",
        use_legacy_sql=False,
        depends_on_past=False,
        wait_for_downstream=False,
        on_failure_callback=callbacks.task_fail_slack_alert(
            "U03BANPLXJR",
            channel=env,  # Carlos
        ),
    )

    dates_tier_factor_check = BigQueryValueCheckOperator(
        gcp_conn_id="google_cloud_olvin_default",
        sql="{% include './bigquery/adjustments/events/tests/factor_per_tier_date/correct_dates.sql' %}",
        pass_value=0,
        task_id="dates_tier_factor_check",
        dag=dag,
        location="EU",
        use_legacy_sql=False,
        depends_on_past=False,
        wait_for_downstream=False,
        on_failure_callback=callbacks.task_fail_slack_alert(
            "U03BANPLXJR",
            channel=env,  # Carlos
        ),
    )

    manual_factors_check = BigQueryValueCheckOperator(
        gcp_conn_id="google_cloud_olvin_default",
        sql="{% include './bigquery/adjustments/events/tests/factor_per_tier_date/count_manual_factors.sql' %}",
        pass_value=False,
        task_id="manual_factors_check",
        dag=dag,
        location="EU",
        use_legacy_sql=False,
        depends_on_past=False,
        wait_for_downstream=False,
        on_failure_callback=callbacks.task_fail_slack_alert(
            "U03BANPLXJR",
            channel=env,  # Carlos
        ),
    )

    factor_per_poi_event = OlvinBigQueryOperator(
        task_id="factor_per_poi_event",
        query="{% include './bigquery/adjustments/events/create_factor_per_event_place.sql' %}",
        dag=dag,
    )

    (
        move_events_to_accessible_by_olvin
        >> move_factors_from_acc_olvin
        >> [
            same_tier_ids_check,
            no_nulls_tier_factor_check,
            dates_tier_factor_check,
            manual_factors_check,
        ]
        >> factor_per_poi_event
    )

    dates_poi_factor_check = BigQueryValueCheckOperator(
        gcp_conn_id="google_cloud_olvin_default",
        sql="{% include './bigquery/adjustments/events/tests/factor_per_event_place/correct_dates.sql' %}",
        pass_value=0,
        task_id="dates_poi_factor_check",
        dag=dag,
        location="EU",
        use_legacy_sql=False,
        depends_on_past=False,
        wait_for_downstream=False,
        on_failure_callback=callbacks.task_fail_slack_alert(
            "U03BANPLXJR",
            channel=env,  # Carlos
        ),
    )

    no_duplicates_poi_factor_check = BigQueryValueCheckOperator(
        gcp_conn_id="google_cloud_olvin_default",
        sql="{% include './bigquery/adjustments/events/tests/factor_per_event_place/no_duplicates.sql' %}",
        pass_value=0,
        task_id="no_duplicates_poi_factor_check",
        dag=dag,
        location="EU",
        use_legacy_sql=False,
        depends_on_past=False,
        wait_for_downstream=False,
        on_failure_callback=callbacks.task_fail_slack_alert(
            "U03BANPLXJR",
            channel=env,  # Carlos
        ),
    )

    factor_per_poi_event >> [dates_poi_factor_check, no_duplicates_poi_factor_check]

    ref_visits_events_query = OlvinBigQueryOperator(
        task_id="ref_visits_events",
        query="{% include './bigquery/adjustments/events/create_ref_visits_per_event_place.sql' %}",
        dag=dag,
        billing_tier="med",
    )

    start >> ref_visits_events_query

    number_rows_ref_visits_check = BigQueryValueCheckOperator(
        gcp_conn_id="google_cloud_olvin_default",
        sql="{% include './bigquery/adjustments/events/tests/ref_visits/check_number_rows.sql' %}",
        pass_value=0,
        task_id="number_rows_ref_visits_check",
        dag=dag,
        location="EU",
        use_legacy_sql=False,
        depends_on_past=False,
        wait_for_downstream=False,
        on_failure_callback=callbacks.task_fail_slack_alert(
            "U03BANPLXJR",  # Carlos
            channel=env,
        ),
    )

    ref_visits_events_query >> number_rows_ref_visits_check

    same_size_refvisits_factor_check = BigQueryValueCheckOperator(
        gcp_conn_id="google_cloud_olvin_default",
        sql="{% include './bigquery/adjustments/events/tests/ref_visits/check_size_factor_ref_visits.sql' %}",
        pass_value=0,
        task_id="same_size_refvisits_factor_check",
        dag=dag,
        location="EU",
        use_legacy_sql=False,
        depends_on_past=False,
        wait_for_downstream=False,
        on_failure_callback=callbacks.task_fail_slack_alert(
            "U03BANPLXJR",  # Carlos
            channel=env,
        ),
    )

    [factor_per_poi_event, ref_visits_events_query] >> same_size_refvisits_factor_check

    scale_event_visits = OlvinBigQueryOperator(
        task_id="scale_event_visits",
        query="{% include './bigquery/adjustments/events/scale_event_visits.sql' %}",
        dag=dag,
        billing_tier="med",
    )
    [
        dates_poi_factor_check,
        no_duplicates_poi_factor_check,
        same_size_refvisits_factor_check,
        number_rows_ref_visits_check,
    ] >> scale_event_visits

    move_adjustments_events_usage_table = SNSBigQueryOperator(
        task_id="move_adjustments_events_usage_table",
        query="{% include './bigquery/adjustments/events/sns/copy_usage_table.sql' %}",
        dag=dag,
    )
    scale_event_visits >> move_adjustments_events_usage_table

    no_duplicates_out_visits_check = BigQueryValueCheckOperator(
        gcp_conn_id="google_cloud_olvin_default",
        sql="{% include './bigquery/adjustments/events/tests/events_adjusted_visits/no_duplicates.sql' %}",
        pass_value=0,
        task_id="no_duplicates_out_visits_check",
        dag=dag,
        location="EU",
        use_legacy_sql=False,
        depends_on_past=False,
        wait_for_downstream=False,
        on_failure_callback=callbacks.task_fail_slack_alert(
            "U03BANPLXJR",
            channel=env,  # Carlos
        ),
    )

    no_nulls_out_visits_check = BigQueryValueCheckOperator(
        gcp_conn_id="google_cloud_olvin_default",
        sql="{% include './bigquery/adjustments/events/tests/events_adjusted_visits/no_nulls.sql' %}",
        pass_value=0,
        task_id="no_nulls_out_visits_check",
        dag=dag,
        location="EU",
        use_legacy_sql=False,
        depends_on_past=False,
        wait_for_downstream=False,
        on_failure_callback=callbacks.task_fail_slack_alert(
            "U03BANPLXJR",
            channel=env,  # Carlos
        ),
    )

    same_pids_out_check = BigQueryValueCheckOperator(
        gcp_conn_id="google_cloud_olvin_default",
        sql="{% include './bigquery/adjustments/events/tests/events_adjusted_visits/same_poi_ids.sql' %}",
        pass_value=0,
        task_id="same_pids_out_check",
        dag=dag,
        location="EU",
        use_legacy_sql=False,
        depends_on_past=False,
        wait_for_downstream=False,
        on_failure_callback=callbacks.task_fail_slack_alert(
            "U03BANPLXJR",
            channel=env,  # Carlos
        ),
    )

    only_holidays_changed_check = BigQueryValueCheckOperator(
        gcp_conn_id="google_cloud_olvin_default",
        sql="{% include './bigquery/adjustments/events/tests/events_adjusted_visits/only_holidays_changed.sql' %}",
        pass_value=0,
        task_id="only_holidays_changed_check",
        dag=dag,
        location="EU",
        use_legacy_sql=False,
        depends_on_past=False,
        wait_for_downstream=False,
        on_failure_callback=callbacks.task_fail_slack_alert(
            "U03BANPLXJR",
            channel=env,  # Carlos
        ),
    )

    number_rows_events_output_check = BigQueryValueCheckOperator(
        gcp_conn_id="google_cloud_olvin_default",
        sql="{% include './bigquery/adjustments/events/tests/events_adjusted_visits/check_number_rows.sql' %}",
        pass_value=0,
        task_id="number_rows_events_output_check",
        dag=dag,
        location="EU",
        use_legacy_sql=False,
        depends_on_past=False,
        wait_for_downstream=False,
        on_failure_callback=callbacks.task_fail_slack_alert(
            "U03BANPLXJR",  # Carlos
            channel=env,
        ),
    )
    create_ts_brands_events = OlvinBigQueryOperator(
        task_id="create_ts_brands_events",
        query="{% include './bigquery/metrics/brand_agg/events.sql' %}",
        billing_tier="med",
    )

    mark_success_events = MarkSuccessOperator(task_id="mark_success_events", dag=dag)

    (
        scale_event_visits
        >> [
            no_duplicates_out_visits_check,
            no_nulls_out_visits_check,
            same_pids_out_check,
            only_holidays_changed_check,
            number_rows_events_output_check,
        ]
        >> create_ts_brands_events
        >> mark_success_events
    )

    step = "adjustments_events"

    validations_start = DummyOperator(task_id=f"{step}_validation_start")
    mark_success_events >> validations_start

    events_validation = visits_estimation_validation(
        input_table=f"{Variable.get('env_project')}.{dag.params['visits_estimation_dataset']}.{dag.params['adjustments_events_output_table']}",
        env=env,
        pipeline="visits_estimation",
        step=step,
        run_date="{{ ds }}",
        classes=["fk_sgbrands", "naics_code", "region", "top_category"],
        start_task=validations_start,
    )

    mark_success_events_validation = MarkSuccessOperator(
        task_id=f"mark_success_{step}_validation",
        on_failure_callback=check_dashboard_slack_alert(
            "U03BANPLXJR", data_studio_link="https://lookerstudio.google.com/u/0/reporting/07196097-8f5b-41d6-a6bf-b1b261bfe89d/page/p_j2j42t5i4c", message=f"Check metrics on {step}."
        ),
        dag=dag,
    )

    events_validation >> mark_success_events_validation

    return mark_success_events_validation
