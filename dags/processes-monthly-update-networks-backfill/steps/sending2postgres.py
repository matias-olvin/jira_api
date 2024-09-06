"""
DAG ID: demographics_pipeline
"""
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


def register(dag, start):
    """Register tasks on the dag.

    Args:
        dag (airflow.models.DAG): DAG instance to register tasks on.
        start (airflow.models.TaskInstance): Task instance all tasks will
            registered downstream from.

    Returns:
        airflow.models.TaskInstance: The last task node in this section.
    """
    final_tasks = []

    def results_networks(
        dataset_bigquery: str, table_bigquery: str, database_table: str
    ):
        # Export to GCS
        export_job = BigQueryInsertJobOperator(
            task_id=f"export_{database_table}",
            configuration={
                "extract": {
                    "destinationUri": (
                        (
                            f"gs://{{{{ params['neo4j_bucket'] }}}}/postgres/{{{{ ds.format('%Y-%m-01') }}}}/{database_table}/*.csv"
                        )
                    ),
                    "printHeader": True,
                    "destinationFormat": "CSV",
                    "fieldDelimiter": "\t",
                    "sourceTable": {
                        "projectId": "{{ params['project'] }}",
                        "datasetId": dataset_bigquery,
                        "tableId": table_bigquery,
                    },
                },
                "labels": {
                    "pipeline": "{{ dag.dag_id }}",
                    "task_id": "{{ task.task_id.lower()[:63] }}",
                },
            },
            dag=dag,
        )
        start >> export_job

        # Delete possible matching data from database
        delete_operator = PostgresOperator(
            task_id=f"delete_{database_table}",
            dag=dag,
            postgres_conn_id=dag.params["postgres_conn_id"],
            sql=f"TRUNCATE TABLE {database_table.lower()} ",
        )
        export_job >> delete_operator

        # Upload new data to database
        def copy_to_gcs(bucket_name, gcs_folder, table_name):
            """Python function to copy files one by one form GCS and upload them to Postgres"""
            import csv
            import logging
            from tempfile import NamedTemporaryFile

            import pandas as pd
            from airflow.providers.google.cloud.hooks.gcs import GCSHook
            from airflow.providers.postgres.hooks.postgres import PostgresHook

            # Hooks with functions to connect to Postgres and GCS
            pg_hook = PostgresHook.get_hook(
                dag.params["postgres_conn_id"]
            )  # Name of the connection
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
                    # Dropping a column
                    df = pd.read_csv(temp_name, sep="\t", index_col=False)
                    df = df.drop(columns="fk_met_areas", errors="ignore")
                    df.to_csv(temp_name, index=False, sep="\t", header=True)
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
            task_id=f"upload_{database_table}",
            dag=dag,
            python_callable=copy_to_gcs,  # The function previously defined
            op_kwargs={
                "bucket_name": "{{ params['neo4j_bucket'] }}",  # Source bucket
                "gcs_folder": f"postgres/{{{{ ds.format('%Y-%m-01') }}}}/{database_table}",  # Source folder
                "table_name": f"{database_table.lower()}",  # Destination table
            },
        )
        delete_operator >> upload_operator
        # Delete staging data
        delete_staging = GCSDeleteObjectsOperator(
            task_id=f"delete_staging_{database_table}",
            depends_on_past=False,
            dag=dag,
            bucket_name=dag.params["neo4j_bucket"],
            prefix=f"postgres/{{{{ ds.format('%Y-%m-01') }}}}/{database_table}/",
        )
        upload_operator >> delete_staging
        final_tasks.append(delete_staging)

    #     # Networks
    results_networks(
        "{{ params['postgres_dataset'] }}",
        "{{ params['postgres_connection_table'] }}",
        "SGPlaceConnection",
    )
    #     results_networks("{{ params['networks_dataset'] }}", "{{ params['chain_connections_all_table'] }}", "SGChainConnection")
    #     results_networks("{{ params['networks_dataset'] }}", "{{ params['journeys_all_table'] }}", "SGPlaceJourney")

    return final_tasks
