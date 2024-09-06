# from google.cloud import bigquery
# from google.cloud.bigquery.job import QueryJobConfig


# def extract_pipeline_tables(pipeline: str, creation_time_filter: str, run_type: str, **kwargs):   # -> pushes result to XCom
#     """
#     Query `region-eu`.INFORMATION_SCHEMA.JOBS_BY_PROJECT

#     Args:
#         pipeline_id (str): shorthand of pipeline to filter query results by.

#     Returns:
#         str: string of tables pushed to xcom.
#     """
#     # Initialise string to store query results
#     tables_in_pipeline = ""
#     # Construct a BigQuery client object
#     client = bigquery.Client(
#         project='compute-prod-olvin-com'
#     )
#     # Query to run
#     query = f"""
#         SELECT
#             CONCAT(
#                 destination_table.project_id, '.',
#                 destination_table.dataset_id, '.',
#                 SPLIT(destination_table.table_id, '$')[OFFSET(0)]
#             ) AS tables
#         FROM
#             `region-eu`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
#         WHERE
#             destination_table.table_id IS NOT NULL AND
#             job_id LIKE '%airflow_{pipeline}%' AND
#             user_email LIKE 'composer%' AND
#             {creation_time_filter}
#     """
#     # Make an API request
#     query_job = client.query(
#         query=query,
#         job_config=QueryJobConfig(
#             labels={
#                 f'{run_type}_metadata': 'pipeline'
#             }
#         )
#     )
#     # Add results to tables_in_pipeline string
#     for row in query_job:
#         tables_in_pipeline += f"'{row[0]}',"

#     # Push to XCom
#     kwargs['ti'].xcom_push(key='tables_in_pipeline', value=tables_in_pipeline[:-1])


# def extract_storage_bytes(tables: str, run_type: str, **kwargs):   # -> pushes result to XCom
#     """
#     Query `region-eu`.INFORMATION_SCHEMA.TABLE_STORAGE

#     Args:
#         tables (str): tables sued by pipeline.

#     Returns:
#         str: string containf  sum of active_physical_bytes for
#         all tables and long_term_physical_bytes for all tables.
#     """
#     # Projects to check for tables
#     projects = [
#         'compute-prod-olvin-com',
#         'storage-prod-olvin-com',
#         'storage-dev-olvin-com'
#     ]
#     # Create bigquery.Client for each project
#     clients = [bigquery.Client(project=project) for project in projects]
#     # Query to run
#     query = f"""
#         SELECT
#             SUM(active_physical_bytes) AS active_physical_bytes,
#             SUM(long_term_physical_bytes) AS long_term_physical_bytes
#         FROM (
#             SELECT
#                 CONCAT(project_id, '.', table_schema, '.', table_name) AS _table,
#                 SUM(active_physical_bytes) AS active_physical_bytes,
#                 SUM(long_term_physical_bytes) AS long_term_physical_bytes
#             FROM `region-eu`.INFORMATION_SCHEMA.TABLE_STORAGE
#             GROUP BY
#                 project_id,
#                 table_schema,
#                 table_name
#         )
#         WHERE _table IN ({tables})
#     """
#     # Initialise variables
#     active_physical_bytes = 0
#     long_term_physical_bytes = 0
#     # Loop through each client
#     for client in clients:
#         # Make an API request.
#         query_job = client.query(
#             query=query,
#             job_config=QueryJobConfig(
#                 labels={
#                     f'{run_type}_metadata': 'pipeline'
#                 }
#             )
#         )
#         for row in query_job:
#             # Add results to variables
#             if row[0] is not None:
#                 active_physical_bytes += row[0]
#             if row[1] is not None:
#                 long_term_physical_bytes += row[1]

#     # Push to XCom
#     kwargs['ti'].xcom_push(key='active_physical_bytes', value=active_physical_bytes)
#     kwargs['ti'].xcom_push(key='long_term_physical_bytes', value=long_term_physical_bytes)
