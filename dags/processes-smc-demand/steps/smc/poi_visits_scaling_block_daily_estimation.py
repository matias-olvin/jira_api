import pandas as pd
import pendulum
from airflow.models import DAG, TaskInstance, Variable
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from common.operators.bigquery import OlvinBigQueryOperator


def register(start: TaskInstance, dag: DAG) -> TaskGroup:

    years_compute = pd.date_range(
        "2018-01-01", Variable.get("smc_demand_start_date"), freq="YS", inclusive="left"
    )

    # these dummy starts serve to create tasks in series through a loop
    dummy_start_1 = start
    dummy_start_2 = start

    for year_start in years_compute:
        year_name = year_start.strftime("%Y")

        with TaskGroup(
            group_id=f"smc_poi_visits_scaled_daily_estimation_{year_name}_task_group"
        ) as group:

            for month in range(1, 13):
                date_start = pendulum.date(year=int(year_name), month=month, day=1)

                end_date = date_start.end_of("month")

                current_date = date_start

                date_list_params = list()

                while current_date <= end_date:
                    date_list_params.append(
                        {
                            "date_start": current_date.to_date_string(),
                            "year_name": year_name,
                        }
                    )
                    current_date = current_date.add(days=1)

                query_poi_visits_scaled = OlvinBigQueryOperator.partial(
                    task_id=f"smc_query_poi_visits_scaled_block_daily_estimation_{year_name}_{month}",
                    query="{% include './include/bigquery/smc/poi_visits_scaling_block_daily_estimation.sql' %}",
                    retries=3,
                ).expand(params=date_list_params)

                sleep_task = BashOperator(
                    task_id=f"smc_cool_down_period_block_daily_estimation_{month}",
                    bash_command="sleep 120",
                    dag=dag,
                )

                dummy_start_1 >> query_poi_visits_scaled >> sleep_task

                # added to make the monthly tasks in series rather than parallel
                dummy_start_1 = sleep_task

        dummy_start_2 >> group

        # added to make the yearly tasks in series rather than parallel
        dummy_start_2 = group

    return group
