# from google.cloud import bigquery
# from google.cloud.bigquery.job import QueryJobConfig


# def extract_billing_info(dag, pipeline:str, pipeline_label: str, run_date: str, date_filter:str, other=False, **kwargs):
#     """

#     """
#     client = bigquery.Client(project=f"{dag.params['project_id']}")

#     resources = {
#         "BigQuery": [
#             "Analysis",
#             "Active Storage",
#             "Long Term Storage"
#         ],
#         "Cloud Storage": ["%"],
#         "Compute Engine": ["%"]
#     }

#     other = False
#     if pipeline == 'other':
#         other = True

#     def query(get_bytes=False, get_costs=False):
#         if get_bytes:
#             agg_col = 'usage.amount'
#         elif get_costs:
#             agg_col = 'cost'

#         # query to extract pipeline data
#         run_query = f"""
#             SELECT
#                 IFNULL(SUM({agg_col}), 0)
#             FROM
#                 `{dag.params['project_id']}.{dag.params['gcp_billing_dataset']}.{dag.params['gcp_billing_table']}`
#             WHERE
#                 EXISTS (
#                     SELECT
#                         1
#                     FROM
#                         UNNEST(labels) AS labels
#                     WHERE (labels.key, labels.value) = ('pipeline', '{pipeline_label}')
#                 ) AND
#                 {date_filter} AND
#                 service.description = '{service}' AND
#                 sku.description LIKE '{sku}'
#         """

#         # query to extract non-pipeline data
#         other_query = f"""
#             SELECT
#                 IFNULL(SUM({agg_col}), 0)
#             FROM
#                 `{dag.params['project_id']}.{dag.params['gcp_billing_dataset']}.{dag.params['gcp_billing_table']}`
#             WHERE
#                 NOT EXISTS (
#                     SELECT
#                         1
#                     FROM
#                         UNNEST(labels) AS labels
#                     WHERE
#                         (labels.key) != ('pipeline')
#                 ) AND
#                 {date_filter} AND
#                 service.description = '{service}' AND
#                 sku.description LIKE '{sku}'
#         """

#         if other:
#             return other_query
#         else:
#             return run_query

#     for service in resources:
#         for sku in resources[service]:
#             bytes_query = query(get_bytes=True)
#             costs_query = query(get_costs=True)

#             if sku == "%":
#                 col_name = service.lower().replace(' ', '_')
#             else:
#                 col_name = sku.lower().replace(' ', '_')

#             bytes_col = col_name + '_bytes'
#             costs_col = col_name + '_costs'

#             job_config = QueryJobConfig(
#                 labels={"pipeline": "daily_metadata"}
#             )
#             bytes_query_job = client.query(
#                 query=bytes_query,
#                 job_config=job_config
#             )
#             costs_query_job = client.query(
#                 query=costs_query,
#                 job_config=job_config
#             )

#             for row in bytes_query_job:
#                 if row[0] == None:
#                     bytes_result = 0
#                 else:
#                     bytes_result = row[0]

#             for row in costs_query_job:
#                 if row[0] == None:
#                     costs_result = 0
#                 else:
#                     costs_result = row[0]

#             update_query = f"""
#                 UPDATE
#                     `{dag.params['project_id']}.{dag.params['pipeline_tracking_dataset']}.{dag.params['pipeline_metadata_table']}`
#                 SET
#                     {bytes_col} = {bytes_result},
#                     {costs_col} = {costs_result}
#                 WHERE
#                     run_date = {run_date} AND
#                     pipeline = '{pipeline}'
#             """

#             client.query(
#                 query=update_query,
#                 job_config=job_config
#             )
