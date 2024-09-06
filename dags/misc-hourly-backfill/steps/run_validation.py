from airflow.models import TaskInstance, DAG
from common.operators.validations import TrendValidationOperator
from common.operators.bigquery import OlvinBigQueryOperator


def register(start: TaskInstance, dag: DAG) -> TaskInstance:

    run_trend_validation = TrendValidationOperator(
        task_id="run_trend_validation",
        sub_validation="group",
        env="prod",
        pipeline="postgres",
        step="raw",
        granularity="hour",
        run_date="{{ ds }}",
        source="storage-prod-olvin-com.accessible-by-sns.SGPlaceHourlyVisitsRaw_bf",
        destination="sns-vendor-olvin-poc.accessible_by_olvin.postgres_metrics-trend",
        date_ranges=[["{{ ds }}", "{{ (macros.datetime.strptime(ds, '%Y-%m-%d').replace(day=7)).strftime('%Y-%m-%d') }}"]],
        classes=["fk_sgbrands", "naics_code", "region", "top_category"],
    )

    insert_validation_table = OlvinBigQueryOperator(
        task_id="insert_validation_table",
        query="{% include './include/insert_validation_table.sql' %}",
    )

    start >> run_trend_validation >> insert_validation_table

    return insert_validation_table
