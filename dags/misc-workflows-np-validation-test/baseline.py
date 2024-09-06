import os,pendulum
from common.utils import dag_args, callbacks
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from common.operators.bigquery import (
    OlvinBigQueryOperator,
    SNSBigQueryOperator,
)
from common.operators.validations import (
    TrendValidationOperator,
    VolumeValidationOperator,
)
path = f"{os.path.dirname(os.path.realpath(__file__))}"
steps_path = path + "/steps"

DAG_ID = path.split("/")[-1]

env_args = dag_args.make_env_args(
    dag_id=DAG_ID,
)


with DAG(
    env_args["dag_id"],
    default_args=dag_args.make_default_args(
    start_date=pendulum.datetime(2022, 9, 20, tz="Europe/London"),
    retries=3,
),
    schedule_interval=env_args["schedule_interval"],
    tags=["dev", "test", "validation"],
) as dag:
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    # TREND VALIDATION

    trend_day = TrendValidationOperator(
        task_id="trend_day",
        sub_validation="group",
        env="dev",
        pipeline="neural_prophet",
        step="neural_prophet_half_epochs",
        run_date="2023-04-24",
        source="sns-vendor-olvin-poc.visits_estimation_geospatial_dev.model_output",
        destination="sns-vendor-olvin-poc.accessible_by_olvin.visits_estimation_metrics-np_trend_validation_test",
        granularity="day",
        date_ranges=[["2022-04-01", "2022-06-30"]],
        classes=["fk_sgbrands"],
    )
    start >> trend_day

    trend_week = TrendValidationOperator(
        task_id="trend_week",
        sub_validation="group",
        env="dev",
        pipeline="neural_prophet",
        step="neural_prophet_half_epochs",
        run_date="2023-04-24",
        source="sns-vendor-olvin-poc.visits_estimation_geospatial_dev.model_output",
        destination="sns-vendor-olvin-poc.accessible_by_olvin.visits_estimation_metrics-np_trend_validation_test",
        granularity="week",
        date_ranges=[["2022-01-02", "2022-07-02"]],
        classes=["fk_sgbrands"],
    )
    trend_day >> trend_week

    trend_month = TrendValidationOperator(
        task_id="trend_month",
        sub_validation="group",
        env="dev",
        pipeline="neural_prophet",
        step="neural_prophet_half_epochs",
        run_date="2023-04-24",
        source="sns-vendor-olvin-poc.visits_estimation_geospatial_dev.model_output",
        destination="sns-vendor-olvin-poc.accessible_by_olvin.visits_estimation_metrics-np_trend_validation_test",
        granularity="month",
        date_ranges=[["2018-01-01", "2022-06-30"], ["2021-01-01", "2021-12-31"]],
        classes=["fk_sgbrands"],
    )
    trend_week >> trend_month

    copy_trend_to_olvin = OlvinBigQueryOperator(
task_id="copy_to_trend_olvin",
        query="DELETE FROM `storage-dev-olvin-com.visits_estimation_metrics.np_trend_validation_test` WHERE step = 'neural_prophet_half_epochs'; \n\
            INSERT INTO `storage-dev-olvin-com.visits_estimation_metrics.np_trend_validation_test` \n\
                SELECT * FROM `sns-vendor-olvin-poc.accessible_by_olvin.visits_estimation_metrics-np_trend_validation_test`;",
    )
    trend_month >> copy_trend_to_olvin

    drop_sns_trend_staging = SNSBigQueryOperator(
        task_id="drop_sns_trend_staging",
        query="DROP TABLE `sns-vendor-olvin-poc.accessible_by_olvin.visits_estimation_metrics-np_trend_validation_test`;",
    )
    copy_trend_to_olvin >> drop_sns_trend_staging

    drop_sns_trend_staging >> end

    # VOLUME VALIDATION
    copy_volume_to_sns = SNSBigQueryOperator(
        task_id="copy_volume_to_sns",
        query=(
            "CREATE OR REPLACE TABLE `sns-vendor-olvin-poc.visits_estimation_geospatial_dev.model_output_volume`\n"
            "CLUSTER BY fk_sgplaces\n"
            "AS\n"
            "SELECT fk_sgplaces, SUM(visits_estimated) / DATE_DIFF(MAX(local_date), MIN(local_date), day) AS avg_visits\n"
            "FROM  `sns-vendor-olvin-poc.visits_estimation_geospatial_dev.model_output`\n"
            "WHERE\n"
            "  (local_date >= '2020-10-01' OR local_date < '2020-03-01')\n"
            "GROUP BY fk_sgplaces;"
        ),
    )
    start >> copy_volume_to_sns

    volume = VolumeValidationOperator(
        task_id="volume",
        env="dev",
        pipeline="neural_prophet",
        step="neural_prophet_half_epochs",
        run_date="2023-04-24",
        source="sns-vendor-olvin-poc.visits_estimation_geospatial_dev.model_output_volume",
        destination="sns-vendor-olvin-poc.accessible_by_olvin.visits_estimation_metrics-np_volume_validation_test",
        classes=["fk_sgbrands"],
    )
    copy_volume_to_sns >> volume

    copy_volume_to_olvin = OlvinBigQueryOperator(
task_id="copy_to_volume_olvin",
        query="DELETE FROM `storage-dev-olvin-com.visits_estimation_metrics.np_volume_validation_test` WHERE step = 'neural_prophet_half_epochs'; \n\
            INSERT INTO `storage-dev-olvin-com.visits_estimation_metrics.np_volume_validation_test` \
                SELECT * FROM `sns-vendor-olvin-poc.accessible_by_olvin.visits_estimation_metrics-np_volume_validation_test`;",
    )
    volume >> copy_volume_to_olvin

    drop_olvin_volume_staging = SNSBigQueryOperator(
        task_id="drop_olvin_volume_staging",
        query="DROP TABLE `sns-vendor-olvin-poc.visits_estimation_geospatial_dev.model_output_volume`;",
    )
    copy_volume_to_olvin >> drop_olvin_volume_staging

    drop_sns_volume_staging = SNSBigQueryOperator(
        task_id="drop_sns_volume_staging",
        query="DROP TABLE `sns-vendor-olvin-poc.accessible_by_olvin.visits_estimation_metrics-np_volume_validation_test`;",
    )
    copy_volume_to_olvin >> drop_sns_volume_staging

    [drop_olvin_volume_staging, drop_sns_volume_staging] >> end
