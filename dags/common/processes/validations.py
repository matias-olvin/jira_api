from typing import List

from airflow.models import TaskInstance
from airflow.operators.dummy_operator import DummyOperator
from common.utils.formatting import accessible_table_name_format
from common.utils.queries import (
    delete_from_table,
    drop_table,
    write_append,
    write_copy,
)
from common.operators.bigquery import (
    OlvinBigQueryOperator,
    SNSBigQueryOperator,
)
from common.operators.validations import (
    TrendValidationOperator,
    VolumeValidationOperator,
)
from common.vars import OLVIN_PROJECT


def trend_groups_process(
    input_table: str,
    output_table: str,
    env: str,
    pipeline: str,
    step: str,
    run_date: str,
    classes: List[str],
    start_task: TaskInstance,
) -> DummyOperator:
    """
    Process for running trend validations for groups.
    """
    trend_start = DummyOperator(task_id=f"{step}_trend_validation_start")
    start_task >> trend_start

    day_validation = TrendValidationOperator(
        task_id=f"{step}_trend_day",
        sub_validation="group",
        env=env,
        pipeline=pipeline,
        step=step,
        run_date=run_date,
        source=accessible_table_name_format("sns", input_table),
        destination=accessible_table_name_format("olvin", output_table),
        granularity="day",
        date_ranges=[["2022-04-01", "2022-06-30"]],
        classes=classes,
    )
    trend_start >> day_validation

    week_validation = TrendValidationOperator(
        task_id=f"{step}_validation_trend_week",
        sub_validation="group",
        env=env,
        pipeline=pipeline,
        step=step,
        run_date=run_date,
        source=accessible_table_name_format("sns", input_table),
        destination=accessible_table_name_format("olvin", output_table),
        granularity="week",
        date_ranges=[["2022-01-02", "2022-07-02"]],
        classes=classes,
    )
    day_validation >> week_validation

    month_validation = TrendValidationOperator(
        task_id=f"{step}_validation_trend_month",
        sub_validation="group",
        env=env,
        pipeline=pipeline,
        step=step,
        run_date=run_date,
        source=accessible_table_name_format("sns", input_table),
        destination=accessible_table_name_format("olvin", output_table),
        granularity="month",
        date_ranges=[["2018-01-01", "2022-06-30"], ["2021-01-01", "2021-12-31"]],
        classes=classes,
    )
    week_validation >> month_validation

    clear_olvin = OlvinBigQueryOperator(
        task_id=f"clear_{step}_trend_validation_output_to_olvin",
        query=delete_from_table(
            table=output_table,
            filter_by={
                "env": env,
                "pipeline": pipeline,
                "step": step,
                "run_date": run_date,
            },
        ),
    )
    month_validation >> clear_olvin

    copy_to_olvin = OlvinBigQueryOperator(
        task_id=f"copy_{step}_trend_validation_output_to_olvin",
        query=write_append(
            source=accessible_table_name_format("olvin", output_table),
            destination=output_table,
            filter_by={
                "env": env,
                "pipeline": pipeline,
                "step": step,
                "run_date": run_date,
            },
        ),
    )
    clear_olvin >> copy_to_olvin

    delete_from_staging_sns = SNSBigQueryOperator(
        task_id=f"delete_{step}_trend_validation_output_staging",
        query=delete_from_table(
            table=accessible_table_name_format("olvin", output_table),
            filter_by={
                "env": env,
                "pipeline": pipeline,
                "step": step,
                "run_date": run_date,
            },
        ),
    )
    copy_to_olvin >> delete_from_staging_sns

    trend_end = DummyOperator(task_id=f"{step}_trend_validation_end")
    delete_from_staging_sns >> trend_end

    return trend_end


def trend_poi_process():
    """
    ...
    """
    ...


def trend_groups_hourly_process():
    """
    ...
    """
    # migrate to sns
    ...
    # hourly validation
    ...
    # migrate to olvin
    ...
    # delete staging
    ...
    # marksuccess
    ...


def volume_process(
    input_table: str,
    output_table: str,
    env: str,
    pipeline: str,
    step: str,
    run_date: str,
    classes: List[str],
    start_task: TaskInstance,
) -> DummyOperator:
    """
    Process for running trend validations for groups.
    """
    # Both explode and aggregate visits needed whilst volume in development.
    volume_start = DummyOperator(task_id=f"{step}_volume_validation_start")
    start_task >> volume_start

    # agg and migrate

    validation = VolumeValidationOperator(
        task_id=f"{step}_volume",
        env=env,
        pipeline=pipeline,
        step=step,
        run_date=run_date,
        source=f'{accessible_table_name_format("sns", input_table)}',
        destination=accessible_table_name_format("olvin", output_table),
        classes=classes,
    )
    volume_start >> validation

    clear_olvin = OlvinBigQueryOperator(
        task_id=f"clear_{step}_volume_validation_output_to_olvin",
        query=delete_from_table(
            table=output_table,
            filter_by={
                "env": env,
                "pipeline": pipeline,
                "step": step,
                "run_date": run_date,
            },
        ),
    )
    validation >> clear_olvin

    copy_to_olvin = OlvinBigQueryOperator(
        task_id=f"copy_{step}_volume_validation_output_to_olvin",
        query=write_append(
            source=accessible_table_name_format("olvin", output_table),
            destination=output_table,
            filter_by={
                "env": env,
                "pipeline": pipeline,
                "step": step,
                "run_date": run_date,
            },
        ),
    )
    clear_olvin >> copy_to_olvin

    delete_from_staging_sns = SNSBigQueryOperator(
        task_id=f"delete_{step}_volume_validation_output_staging",
        query=delete_from_table(
            table=accessible_table_name_format("olvin", output_table),
            filter_by={
                "env": env,
                "pipeline": pipeline,
                "step": step,
                "run_date": run_date,
            },
        ),
    )
    copy_to_olvin >> delete_from_staging_sns

    volume_end = DummyOperator(task_id=f"{step}_volume_validation_end")
    delete_from_staging_sns >> volume_end

    return volume_end


def visits_estimation_validation(
    input_table: str,
    env: str,
    pipeline: str,
    step: str,
    run_date: str,
    classes: List[str],
    start_task: TaskInstance,
    trend_output_table: str = \
        f"{OLVIN_PROJECT}.visits_estimation_metrics.ground_truth_trend",
    volume_output_table: str = \
        f"{OLVIN_PROJECT}.visits_estimation_metrics.ground_truth_volume",
):
    copy_to_sns = OlvinBigQueryOperator(
        task_id=f"copy_{step}_validation_input_to_sns",
        query=write_copy(
            source=input_table,
            destination=accessible_table_name_format("sns", input_table),
        ),
    )
    start_task >> copy_to_sns

    trend = trend_groups_process(
        input_table,
        trend_output_table,
        env,
        pipeline,
        step,
        run_date,
        classes,
        copy_to_sns,
    )
    volume = volume_process(
        input_table,
        volume_output_table,
        env,
        pipeline,
        step,
        run_date,
        classes,
        copy_to_sns,
    )

    drop_from_staging_olvin = OlvinBigQueryOperator(
        task_id=f"drop_{step}_trend_validation_input_staging",
        query=drop_table(accessible_table_name_format("sns", input_table)),
    )
    [trend, volume] >> drop_from_staging_olvin

    return drop_from_staging_olvin
