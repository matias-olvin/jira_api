"""
DAG ID: kpi_pipeline
"""
from datetime import datetime, timedelta

from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from common.operators.bigquery import OlvinBigQueryOperator
from common.processes.validations import trend_groups_process, volume_process


def register(dag, start, step):
    def get_dataset(step: str) -> str:
        if step == "raw":
            return "postgres"
        elif step == "final":
            return "postgres_batch"

    def gcs_bucket(step: str) -> str:
        if step == "raw":
            return "postgres-database-tables"
        elif step == "final":
            return "postgres-final-database-tables"

    def prev_month() -> str:
        date_obj = datetime.strptime(Variable.get("monthly_update"), "%Y-%m-%d").date()
        prev_month_date = (date_obj - timedelta(days=1)).replace(day=1)
        return prev_month_date.strftime("%Y-%m-%d")

    def prev_data_path() -> str:
        prev_date = prev_month()
        year, month, _ = prev_date.split("-")
        return f"gs://{gcs_bucket(step)}/SGPlaceDailyVisitsRaw/{year}/{int(month)}/*.csv.gz"

    postgres_metrics_start = DummyOperator(task_id=f"postgres_{step}_metrics_start")

    start >> postgres_metrics_start
    
    end = DummyOperator(task_id=f"postgres_{step}_metrics_end")

    if step == "raw":
        # ACTIVITY TABLE
        activity = OlvinBigQueryOperator(
task_id="activity_metrics",
            query="{% include './bigquery/metrics/activity.sql' %}",
        )
        postgres_metrics_start >> activity

        # STABILITY TABLE
        load_stability_staging = OlvinBigQueryOperator(
task_id="load_stability_staging",
            query=(
                f"{{% with uris = '{prev_data_path()}' %}}"
                "{% include './bigquery/metrics/load_stability_staging.sql' %}"
                "{% endwith %}"
            ),
        )
        postgres_metrics_start >> load_stability_staging

        stability = OlvinBigQueryOperator(
task_id="stability_metrics",
            query="{% include './bigquery/metrics/stability.sql' %}",
        )
        load_stability_staging >> stability

        drop_stability_staging = OlvinBigQueryOperator(
task_id="drop_stability_staging",
            query="{% include './bigquery/metrics/drop_stability_staging.sql' %}",
        )
        stability >> drop_stability_staging

        [activity, drop_stability_staging] >> end

    # MOVE TO SNS
    copy = OlvinBigQueryOperator(
task_id=f"copy_to_accessible_{step}",
        query=f"CREATE OR REPLACE TABLE `storage-prod-olvin-com.accessible_by_sns.{get_dataset(step)}-SGPlaceDailyVisitsRaw` \
            COPY `storage-prod-olvin-com.{get_dataset(step)}.SGPlaceDailyVisitsRaw`",
    )
    postgres_metrics_start >> copy

    # TREND TABLE
    trend = trend_groups_process(
        input_table=f"storage-prod-olvin-com.{get_dataset(step)}.SGPlaceDailyVisitsRaw",
        output_table="storage-prod-olvin-com.postgres_metrics.trend",
        env="prod",
        pipeline="postgres",
        step=step,
        run_date=Variable.get("monthly_update"),
        classes=["fk_sgbrands", "naics_code", "region", "top_category"],
        start_task=copy,
    )

    # VOLUME TABLE
    volume = volume_process(
        input_table=f"storage-prod-olvin-com.{get_dataset(step)}.SGPlaceDailyVisitsRaw",
        output_table="storage-prod-olvin-com.postgres_metrics.volume",
        env="prod",
        pipeline="postgres",
        step=step,
        run_date=Variable.get("monthly_update"),
        classes=["fk_sgbrands", "naics_code", "region", "top_category"],
        start_task=copy,
    )

    [trend, volume] >> end

    return end
