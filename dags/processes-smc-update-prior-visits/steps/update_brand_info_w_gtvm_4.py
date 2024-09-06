from airflow.models import TaskInstance
from common.operators.bigquery import OlvinBigQueryOperator
from common.utils.callbacks import task_fail_slack_alert


def register(start: TaskInstance, env: str) -> TaskInstance:

    check_duplicates_prior_brand_visits = OlvinBigQueryOperator(
        task_id="check_duplicates_prior_brand_visits",
        query="{% include './include/bigquery/update_brand_info_w_gtvm_4/checks/check_duplicates_prior_brand_visits.sql' %}",
        on_failure_callback=task_fail_slack_alert(
            "U05FN3F961X", "U03BANPLXJR", channel=env
        ),  # IGNACIO, CARLOS
    )

    check_median_and_sub_cat_count = OlvinBigQueryOperator(
        task_id="check_median_and_sub_cat_count",
        query="{% include './include/bigquery/update_brand_info_w_gtvm_4/checks/check_median_and_sub_cat_count.sql' %}",
        on_failure_callback=task_fail_slack_alert(
            "U05FN3F961X", "U03BANPLXJR", channel=env
        ),  # IGNACIO, CARLOS
    )

    update_brand_info_using_gtvm = OlvinBigQueryOperator(
        task_id="update_brand_info_using_gtvm",
        query="{% include './include/bigquery/update_brand_info_w_gtvm_4/update_brand_info_using_gtvm.sql' %}",
    )

    check_nulls_prior_brand_visits = OlvinBigQueryOperator(
        task_id="check_nulls_prior_brand_visits",
        query="{% include './include/bigquery/update_brand_info_w_gtvm_4/checks/check_nulls_prior_brand_visits.sql' %}",
        on_failure_callback=task_fail_slack_alert(
            "U05FN3F961X", "U03BANPLXJR", channel=env
        ),  # IGNACIO, CARLOS
    )

    # consider adding more checks

    (
        start
        >> [check_duplicates_prior_brand_visits, check_median_and_sub_cat_count]
        >> update_brand_info_using_gtvm
        >> check_nulls_prior_brand_visits
    )

    return check_nulls_prior_brand_visits
