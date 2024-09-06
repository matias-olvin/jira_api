import pandas as pd
import pendulum
from airflow.models import DAG, TaskInstance, Variable
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from common.operators.bigquery import OlvinBigQueryOperator


def register(start: TaskInstance, dag: DAG) -> TaskGroup:

    start_date, end_date = pd.to_datetime(Variable.get("smc_demand_start_date")), pd.to_datetime(Variable.get("smc_demand_end_date"))
    year_ranges_dict = {
        str(year): pd.date_range(
            start=max(start_date, pd.to_datetime(f'{year}-01-01')),
            end=min(end_date, pd.to_datetime(f'{year}-12-31'))
        ).tolist()
        for year in range(start_date.year, end_date.year + 1)
        if pd.to_datetime(f'{year}-01-01') <= end_date
    }

    # these dummy starts serve to create tasks in series through a loop
    dummy_start_1 = start
    dummy_start_2 = start

    for year_name in year_ranges_dict.keys():

        with TaskGroup(
            group_id=f"post_smc_poi_visits_scaled_groundtruth_{year_name}_task_group"
        ) as group:

            dates = year_ranges_dict[year_name]
    
            date_list_params = list()
            for date in dates:
                date_list_params.append(
                    {
                        "date_start": date.strftime('%Y-%m-%d'),
                        "year_name": year_name,
                    }
                )
            month = date.strftime('%m') ## Will just give the month of the latest date but that is okay for post SMC
            query_poi_visits_scaled = OlvinBigQueryOperator.partial(
                task_id=f"post_smc_query_poi_visits_scaled_block_groundtruth_{year_name}_{month}",
                query="{% include  './include/bigquery/post_smc/poi_visits_scaling_block_groundtruth.sql' %}",
                retries=3,
            ).expand(params=date_list_params)

            sleep_task = BashOperator(
                task_id=f"post_smc_cool_down_period_block_groundtruth_{month}",
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
