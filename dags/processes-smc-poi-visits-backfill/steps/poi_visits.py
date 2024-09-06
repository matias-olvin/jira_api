"""
DAG ID: poi_visits_backfill
"""
import calendar
from collections import defaultdict
from datetime import datetime, timedelta

from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


def register(dag, start):
    """
    > For each year and month in the date range, delete the data from the `smc_visits` table, then
    insert the data from the `visits` table

    Args:
      dag: the dag object
      start: The start of the DAG.
    """

    date_format = "%Y-%m-%d"
    month_conversion = {
        "1": "Jan",
        "2": "Feb",
        "3": "Mar",
        "4": "Apr",
        "5": "May",
        "6": "Jun",
        "7": "Jul",
        "8": "Aug",
        "9": "Sep",
        "10": "Oct",
        "11": "Nov",
        "12": "Dec",
    }

    start_date = datetime.strptime("2018-01-01", date_format)
    end_date = datetime.strptime(
        Variable.get("smc_start_date"), date_format
    ) - timedelta(days=9)

    delta = end_date - start_date
    dates = ((start_date + timedelta(days=i)) for i in range(delta.days + 1))

    # > Creating a dictionary of dictionaries. The outer dictionary is keyed by year, and the
    # inner dictionary is keyed by month. The value of the inner dictionary is a list of days.
    #     > This is used to create the date range for the backfill.
    dates_dict = defaultdict(dict)
    years = {date.year for date in dates}
    for year in years:
        if year != end_date.year:
            for month in range(1, 13):
                num_days = calendar.monthrange(year=year, month=month)[1]
                days = [f"{i+1}" for i in range(num_days)]
                dates_dict[f"{year}"].update({f"{month}": days})
        else:
            for month in range(1, end_date.month + 1):
                if month != end_date.month:
                    num_days = calendar.monthrange(year=year, month=month)[1]
                else:
                    num_days = end_date.day
                days = [f"{i+1}" for i in range(num_days)]
                dates_dict[f"{year}"].update({f"{month}": days})

    create_places_and_footprints_view = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="create_places_and_footprints_view",
        configuration={
            "query": {
                "query": "{% include './bigquery/places_and_footprints.sql' %};",
                "useLegacySql": "false",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
    )
    start >> create_places_and_footprints_view

    prev_task = create_places_and_footprints_view
    for year, months in dates_dict.items():
        year_backfill_start = DummyOperator(
            task_id=f"start_{year}",
        )
        year_backfill_end = DummyOperator(
            task_id=f"end_{year}",
        )
        prev_task >> year_backfill_start

        create_year_table = BigQueryInsertJobOperator(
            task_id=f"create_{year}",
            configuration={
                "query": {
                    "query": f"""
                        CREATE OR REPLACE TABLE `{{{{ var.value.env_project }}}}.{{{{ params['smc_visits_dataset'] }}}}.{year}` 
                            COPY `{{{{ var.value.env_project }}}}.{{{{ params['visits_dataset'] }}}}.{year}`;
                        TRUNCATE TABLE `{{{{ var.value.env_project }}}}.{{{{ params['smc_visits_dataset'] }}}}.{year}`;
                    """,
                    "useLegacySql": "false",
                },
                "labels": {
                    "pipeline": "{{ dag.dag_id }}",
                    "task_id": "{{ task.task_id.lower()[:63] }}",
                },
            },
        )
        year_backfill_start >> create_year_table
        prev_task = year_backfill_end

        for month, days in months.items():
            days = [int(day) for day in days]
            month_start_date = datetime(int(year), int(month), min(days)).strftime(
                date_format
            )
            month_end_date = datetime(int(year), int(month), max(days)).strftime(
                date_format
            )

            month_backfill_clear = BigQueryInsertJobOperator(
                task_id=f"delete_{month_conversion[month]}_{year}",
                configuration={
                    "query": {
                        "query": f"""
                            DECLARE d_start DATE DEFAULT CAST('{month_start_date}' as DATE);
                            DECLARE d_end DATE DEFAULT CAST('{month_end_date}' as DATE);
                            DELETE `{{{{ var.value.env_project }}}}.{{{{ params['smc_visits_dataset'] }}}}.{year}`
                            WHERE
                                local_date >= d_start AND
                                local_date <= d_end
                            ;
                        """,
                        "useLegacySql": "false",
                    },
                    "labels": {
                        "pipeline": "{{ dag.dag_id }}",
                        "task_id": "{{ task.task_id.lower()[:63] }}",
                    },
                },
            )
            month_backfill_query = BigQueryInsertJobOperator(
                task_id=f"query_{month_conversion[month]}_{year}",
                project_id="{{ var.value.dev_project_id }}",
                configuration={
                    "query": {
                        "query": f"""
                            DECLARE d DATE DEFAULT CAST('{month_start_date}' as DATE);
                            WHILE d <= '{month_end_date}' DO
                                IF(NOT EXISTS(
                                    SELECT local_date from `{{{{ var.value.env_project }}}}.{{{{ params['smc_visits_dataset'] }}}}.{year}`
                                    WHERE local_date = d)) THEN
                                INSERT INTO `{{{{ var.value.env_project }}}}.{{{{ params['smc_visits_dataset'] }}}}.{year}`
                                (
                                    device_id,
                                    duration,
                                    lat_long_visit_point,
                                    local_date,
                                    local_hour,
                                    visit_ts,
                                    publisher_id,
                                    country,
                                    device_os,
                                    quality_stats,
                                    day_of_week,
                                    fk_sgbrands,
                                    naics_code,
                                    fk_sgplaces,
                                    enclosed,
                                    hour_week,
                                    visit_score,
                                    hour_ts,
                                    parent_bool,
                                    child_bool     
                                )
                                {{% include './bigquery/poi_visits.sql' %}};
                                END IF;
                                SET d = DATE_ADD(d, INTERVAL 1 DAY);
                            END WHILE;
                        """,
                        "useLegacySql": "false",
                    },
                    "labels": {
                        "pipeline": "{{ dag.dag_id }}",
                        "task_id": "{{ task.task_id.lower()[:63] }}",
                    },
                },
            )
            (
                create_year_table
                >> month_backfill_clear
                >> month_backfill_query
                >> year_backfill_end
            )
