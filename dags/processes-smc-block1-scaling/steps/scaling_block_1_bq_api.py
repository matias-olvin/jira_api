from __future__ import annotations

from time import sleep
from typing import List

import pandas as pd
import numpy as np
import pendulum
from airflow.models import DAG, TaskInstance, Variable
from airflow.operators.python import PythonOperator
from google.api_core.exceptions import GoogleAPIError
from google.cloud import bigquery
from google.cloud.bigquery import QueryJobConfig, QueryPriority


def split_date_range(max_dml_jobs_per_table: int, year_start, year_end):
    """
    Split a date range into groups of dates.

    Args:

    max_dml_jobs_per_table (int): The maximum number of DML jobs that can be run on a table.
    year_start (int): The start year of the range of dates to split.
    year_end (int): The end year of the range of dates to split.

    Returns:

    List[Tuple[str, str]]: A list of tuples containing the start and end dates of each group.
    """

    full_date_list = list()

    for i in range(year_start, year_end+1):
        start_date = f"{i}-01-01"
        end_date = f"{i}-12-31"

        date_range = pd.date_range(start_date, end_date, freq="D").strftime("%Y-%m-%d")

        number_of_while_loops = max_dml_jobs_per_table

        while_loops = np.array_split(date_range, number_of_while_loops)

        groups = [(group[0], group[-1]) for group in while_loops]

        full_date_list.extend(groups)

    return full_date_list

def run_query(client: bigquery.Client, query: str, job_id: str) -> None:
    """
    Run a query in BigQuery.

    Args:

    client (bigquery.Client): A BigQuery client.
    query (str): The query to be run in BigQuery.

    """

    try:
        job_config = QueryJobConfig(priority=QueryPriority.BATCH, use_query_cache=False)
        client.query(query=query, job_config=job_config, job_id=job_id)

        print(f"Query {job_id} has been successfully submitted.")

    except GoogleAPIError as e:

        print(f"Query {job_id} has failed to run.")
        print(e)


def block_1_generate_queries(start_date: str, end_date: str, dag: DAG) -> List[str]:
    """
    Generate a list of scaling block 1 queries to be run in BigQuery.

    Args:

    start_date (str): The start date of the range of dates to generate queries for.
    end_date (str): The end date of the range of dates to generate queries for.

    Returns:

    List[str]: A list of queries to be run in BigQuery.

    """
    if start_date > end_date:
        raise ValueError("start_date must be less than or equal to end_date")

    project = Variable.get("env_project")

    queries = list()

    start_year = pendulum.parse(start_date).year
    end_year = pendulum.parse(end_date).year

    split_date_range_list = split_date_range(20, start_year, end_year)

    for start_date, end_date in split_date_range_list:

        start_date = str(start_date)
        end_date = str(end_date)

        year = pendulum.parse(start_date).year

        query = f"""
        DECLARE d DATE DEFAULT CAST('{start_date}' as DATE);

        WHILE d <= '{end_date}' DO
            IF(NOT EXISTS(
                    SELECT local_date
                    FROM `{project}.{dag.params['poi_visits_block_1_dataset']}.{year}`
                    WHERE local_date = d)
            ) THEN
            CALL `{dag.params['block_1_scaling_procedure']}`(
                "{project}.{dag.params['poi_visits_block_1_dataset']}.{year}",
                CAST(d AS STRING),
                "{project}.{dag.params['smc_sg_places_dataset']}.{dag.params['sg_places_filter_table']}",
                "{project}.{dag.params['demographics_dataset']}.{dag.params['zipcode_demographics_table']}",
                "{project}.{dag.params['device_zipcodes_dataset']}",
                "{project}.{dag.params['smc_poi_visits_dataset']}",
                "{project}.{dag.params['sg_base_tables_dataset']}.{dag.params['categories_match_table']}",
                "{project}.{dag.params['sg_base_tables_dataset']}.{dag.params['naics_code_subcategories_table']}",
                "{project}.{dag.params['smc_sg_places_dataset']}.{dag.params['places_dynamic_table']}",
                "{project}.{dag.params['regressors_dataset']}.{dag.params['weather_collection_table']}",
                "{project}.{dag.params['smc_regressors_dataset']}.{dag.params['weather_dictionary_table']}",
                "{project}.{dag.params['regressors_dataset']}.{dag.params['holidays_collection_table']}",
                "{project}.{dag.params['smc_regressors_dataset']}.{dag.params['holidays_dictionary_table']}",
                "{project}.{dag.params['smc_visits_share_dataset']}.{dag.params['visits_share_model']}",
                "{project}.{dag.params['smc_visits_share_dataset']}.{dag.params['prior_distance_parameters_table']}"
            );
            END IF;
            SET d = DATE_ADD(d, INTERVAL 1 DAY);
        END WHILE;
        """

        queries.append(query)

    return queries


def register(
    start: TaskInstance, dag: DAG, env: str, start_date: str, end_date: str
) -> TaskInstance:
    def run_scaling(start_date, end_date, job_id_prefix, env, time_stamp_val, **kwargs):

        execution_date = kwargs["ds_nodash"]

        # Instantiate a BigQuery client
        client = bigquery.Client(project=f"storage-{env}-olvin-com")

        # Create a list of queries to be run
        queries = block_1_generate_queries(start_date, end_date, dag)

        # Run the queries
        for n, query in enumerate(queries):
            
            job_id = f"{job_id_prefix}_{execution_date}_{time_stamp_val}_{n}"

            run_query(client, query, job_id)

            if n % 20 == 0:
                sleep(10)

        client.close()

    def check_scaling(job_id_prefix, env, sleep_interval, time_stamp_val, **kwargs):

        execution_date = kwargs["ds_nodash"]

        job_id_prefix = f"{job_id_prefix}_{execution_date}_{time_stamp_val}"

        # Instantiate a BigQuery client
        client = bigquery.Client(project=f"storage-{env}-olvin-com")

        # Get all jobs with the given prefix
        jobs = client.list_jobs()

        matched_job_ids = [
            job.job_id for job in jobs if job.job_id.startswith(job_id_prefix)
        ]

        # Iterate through the jobs and check their status until all are completed regardless of their status
        while matched_job_ids:

            for job_id in matched_job_ids:

                job = client.get_job(job_id)

                if job.state == "DONE":
                    print(f"Job {job_id} has completed.")
                    matched_job_ids.remove(job_id)

            if matched_job_ids:
                print(
                    f"Jobs remaining: {len(matched_job_ids)}, checking again in {sleep_interval} seconds."
                )
                sleep(sleep_interval)

    job_id_prefix = "smc_block_1_scaling"

    now = pendulum.now()
    formatted_time = now.format("mm_ss")

    run_block_1_scaling_queries = PythonOperator(
        task_id="run_block_1_scaling_queries",
        python_callable=run_scaling,
        op_kwargs={
            "start_date": start_date,
            "end_date": end_date,
            "job_id_prefix": job_id_prefix,
            "env": env,
            "time_stamp_val": formatted_time,
        },
        provide_context=True,
        dag=dag,
    )

    check_status_of_run_block_1_scaling_queries = PythonOperator(
        task_id="check_status_of_run_block_1_scaling_queries",
        python_callable=check_scaling,
        op_kwargs={
            "job_id_prefix": job_id_prefix,
            "env": env,
            "sleep_interval": 300,
            "time_stamp_val": formatted_time,
        },
        provide_context=True,
        dag=dag,
    )

    start >> run_block_1_scaling_queries >> check_status_of_run_block_1_scaling_queries

    return check_status_of_run_block_1_scaling_queries
