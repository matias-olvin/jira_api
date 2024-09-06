import csv
import logging
from tempfile import NamedTemporaryFile

import pandas as pd
from airflow.models import DAG, TaskInstance, Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator


def register(start: TaskInstance, dag: DAG, output_folder: str) -> TaskInstance:
    """Register tasks on the dag.

    Args:
        start (airflow.models.TaskInstance): Task instance all tasks will
            registered downstream from.
        dag (airflow.models.DAG): DAG instance to register tasks on.

    Returns:
        airflow.models.TaskInstance: The last task node in this section.
    """

    tasks = list()
    OUTPUT_DATA_FOLDER = f"{output_folder}/GOLD"

    if output_folder == "historical":
        start_date = "{{ ds }}"
        conn_id = "postgres_conn_id"
    elif output_folder == "forecast":
        start_date = "{{ next_ds }}"
        conn_id = "postgres_conn_id_prod"

    postgres_con = dag.params[conn_id]

    def upload_table(database_name, gcs_name):
        # Delete possible matching data from database
        delete_operator = PostgresOperator(
            task_id=f"delete_{database_name}",
            project_id=Variable.get("compute_project_id"),
            dag=dag,
            postgres_conn_id=postgres_con,
            sql=f"DELETE FROM {database_name} WHERE local_date >= '{start_date}'",
        )

        start >> delete_operator

        # Upload new data to database
        def copy_to_gcs(bucket_name, gcs_folder, table_name):
            """Python function to copy files one by one form GCS and upload them to Postgres"""
            # Hooks with functions to connect to Postgres and GCS
            pg_hook = PostgresHook.get_hook(postgres_con)  # Name of the connection
            gcs_hook = GCSHook()
            # Get list of all CSV files in folder
            file_list = gcs_hook.list(bucket_name, prefix=gcs_folder, delimiter=".csv")
            # Upload one file at a time
            for file_name in file_list:
                # Create temporary file for data from GCS
                with NamedTemporaryFile(suffix=".csv") as temp_file:
                    temp_name = temp_file.name
                    logging.info("Downloading data from file '%s'", file_name)
                    gcs_hook.download(file_name, bucket_name, temp_name)
                    logging.info("Modifying data in pandas")
                    df = pd.read_csv(temp_name, sep="\t", index_col=False)
                    df.drop(
                        columns=["fk_met_areas", "temp_res", "hour_ts"],
                        errors="ignore",
                        inplace=True,
                    )
                    df.to_csv(temp_name, index=False, header=True, sep="\t")
                    logging.info("Uploading data to table '%s'", table_name)
                    # Get list of headers so upload is done in order
                    with open(temp_name, "r") as f:
                        reader = csv.reader(f, delimiter="\t")
                        # The line is a list of keys
                        headers = ", ".join(next(reader))
                    # copy_expert avoids needing superuser permissions
                    pg_hook.copy_expert(
                        rf"COPY {table_name} ({headers}) FROM STDIN WITH CSV HEADER DELIMITER E'\t'",
                        temp_name,
                    )

        upload_operator = PythonOperator(
            task_id=f"upload_{database_name}",
            project_id=Variable.get("compute_project_id"),
            dag=dag,
            python_callable=copy_to_gcs,  # The function previously defined
            op_kwargs={
                "bucket_name": "{{ params['weather_bucket'] }}",  # Source bucket
                "gcs_folder": f"{OUTPUT_DATA_FOLDER}/{gcs_name}/",  # Source folder
                "table_name": f"{database_name}",  # Destination table
            },
        )
        delete_operator >> upload_operator
        tasks.append(upload_operator)

    if output_folder == "historical":
        args_list = [
            ("weathercsddailyhistorical", "csds_daily"),
            ("weatherneighbourhooddailyhistorical", "neighbourhoods_daily"),
        ]
    elif output_folder == "forecast":
        args_list = [
            ("weathercsddaily", "csds_daily"),
            ("weatherneighbourhooddaily", "neighbourhoods_daily"),
            ("weathercsdhourly", "csds_hourly"),
            ("weatherneighbourhoodhourly", "neighbourhoods_hourly"),
        ]
    for args in args_list:
        upload_table(*args)

    return tasks
