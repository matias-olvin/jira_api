from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.bigquery_dts import (
    BigQueryCreateDataTransferOperator,
)
from common.operators.bigquery import OlvinBigQueryOperator


def register(dag, start):
    """Register tasks on the dag.
    Args:
        dag (airflow.models.DAG): DAG instance to register tasks on.
        start (airflow.models.TaskInstance): Task instance all tasks will
            registered downstream from.
    Returns:
        airflow.models.TaskInstance: The last task node in this section.
    """

    create_mv_daily_observed_visits = BigQueryInsertJobOperator(
        gcp_conn_id="google_cloud_olvin_default",
        task_id="create_mv_daily_observed_visits",
        project_id=Variable.get("env_project"),  # "{{ params['compute_project_id'] }}",
        configuration={
            "query": {
                "query": "{% include './bigquery/daily_observed_visits.sql' %}",
                "useLegacySql": "False",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )

    create_mv_monthly_observed_visits = BigQueryInsertJobOperator(
        gcp_conn_id="google_cloud_olvin_default",
        task_id="create_mv_monthly_observed_visits",
        project_id=Variable.get("env_project"),  # "{{ params['compute_project_id'] }}",
        configuration={
            "query": {
                "query": "{% include './bigquery/monthly_observed_visits.sql' %}",
                "useLegacySql": "False",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )

    create_mv_monthly_observed_visits_per_device = BigQueryInsertJobOperator(
        gcp_conn_id="google_cloud_olvin_default",
        task_id="create_mv_monthly_observed_visits_per_device",
        project_id=Variable.get("env_project"),  # "{{ params['compute_project_id'] }}",
        configuration={
            "query": {
                "query": "{% include './bigquery/monthly_observed_visits_per_device.sql' %}",
                "useLegacySql": "False",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )

    create_mv_regressor_weather_daily = BigQueryInsertJobOperator(
        gcp_conn_id="google_cloud_olvin_default",
        task_id="create_mv_regressor_weather_daily",
        project_id=Variable.get("env_project"),  # "{{ params['compute_project_id'] }}",
        configuration={
            "query": {
                "query": "{% include './bigquery/daily_weather.sql' %}",
                "useLegacySql": "False",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )

    create_validations_kpi_dwm_correlations = OlvinBigQueryOperator(
        task_id="create_validations_kpi_dwm_correlations",
        query="{% include './bigquery/create_validations_kpi_dwm_correlations.sql' %}",
    )

    create_validations_kpi_dwm_correlations_latest_mu = OlvinBigQueryOperator(
        task_id="create_validations_kpi_dwm_correlations_latest_mu",
        query="{% include './bigquery/create_validations_kpi_dwm_correlations_latest_mu.sql' %}",
    )

    create_validations_kpi_kpi_v2_table_chart = OlvinBigQueryOperator(
        task_id="create_validations_kpi_kpi_v2_table_chart",
        query="{% include './bigquery/create_validations_kpi_kpi_v2_table_chart.sql' %}",
    )

    create_validations_kpi_kpi_v2_table_chart_full_date_range = OlvinBigQueryOperator(
        task_id="create_validations_kpi_kpi_v2_table_chart_full_date_range",
        query="{% include './bigquery/create_validations_kpi_kpi_v2_table_chart_full_date_range.sql' %}",
    )

    create_mv_validations_almanac_trend = BigQueryInsertJobOperator(
        gcp_conn_id="google_cloud_olvin_default",
        task_id="create_mv_validations_almanac_trend",
        project_id=Variable.get("env_project"),  # "{{ params['compute_project_id'] }}",
        configuration={
            "query": {
                "query": "{% include './bigquery/validations_almanac_trend.sql' %}",
                "useLegacySql": "False",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )

    create_mv_validations_almanac_volume = BigQueryInsertJobOperator(
        gcp_conn_id="google_cloud_olvin_default",
        task_id="create_mv_validations_almanac_volume",
        project_id=Variable.get("env_project"),  # "{{ params['compute_project_id'] }}",
        configuration={
            "query": {
                "query": "{% include './bigquery/validations_almanac_volume.sql' %}",
                "useLegacySql": "False",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )

    create_regressors_holidays = BigQueryCreateDataTransferOperator(
        transfer_config={
            "display_name": "Holidays events regressors",
            "data_source_id": "scheduled_query",
            "schedule": "28 of month 09:00",
            "params": {
                "query": "{% include './bigquery/regressors_holidays.sql' %}",
            },
        },
        project_id=Variable.get("env_project"),
        task_id="create_regressors_holidays",
        dag=dag,
        location="EU",
    )

    create_visits_avg = BigQueryCreateDataTransferOperator(
        transfer_config={
            "display_name": "Average visits",
            "data_source_id": "scheduled_query",
            "schedule": "15 of month 09:00",
            "params": {
                "query": "{% include './bigquery/visits_avg.sql' %}",
            },
        },
        project_id=Variable.get("env_project"),
        task_id="create_visits_avg",
        dag=dag,
        location="EU",
    )

    test_sns_access = BigQueryInsertJobOperator(
        task_id="test_sns_access",
        project_id=Variable.get("env_project"),  # "{{ params['compute_project_id'] }}",
        gcp_conn_id="cross_project_worker_conn",
        configuration={
            "query": {
                "query": "select * from `sns-vendor-olvin-poc.visits_estimation_ground_truth_supervised_dev.class_pois_olvin`",
                "useLegacySql": "False",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )

    test_olvin_access = BigQueryInsertJobOperator(
        task_id="test_olvin_access",
        project_id=Variable.get("env_project"),  # "{{ params['compute_project_id'] }}",
        gcp_conn_id="cross_project_worker_conn",
        configuration={
            "query": {
                "query": "select * from `storage-prod-olvin-com.sg_places.places_dynamic`",
                "useLegacySql": "False",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )

    create_logical_views = BigQueryInsertJobOperator(
        gcp_conn_id="google_cloud_olvin_default",
        task_id="create_logical_views",
        project_id=Variable.get("env_project"),  # "{{ params['compute_project_id'] }}",
        configuration={
            "query": {
                "query": "{% include './bigquery/logical_views.sql' %}",
                "useLegacySql": "False",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )

    end = DummyOperator(task_id="end", dag=dag)

    (
        start
        >> create_mv_daily_observed_visits
        >> create_mv_monthly_observed_visits
        >> create_mv_monthly_observed_visits_per_device
        >> create_mv_regressor_weather_daily
        >> end
    )
    start >> create_regressors_holidays >> create_visits_avg >> end
    start >> test_sns_access >> end
    start >> test_olvin_access >> end
    start >> create_logical_views >> end
    (
        start
        >> create_mv_validations_almanac_trend
        >> create_mv_validations_almanac_volume
        >> create_validations_kpi_dwm_correlations
        >> create_validations_kpi_dwm_correlations_latest_mu
        >> create_validations_kpi_kpi_v2_table_chart
        >> create_validations_kpi_kpi_v2_table_chart_full_date_range
        >> end
    )

    return end
