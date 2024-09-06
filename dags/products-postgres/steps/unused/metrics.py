# """
# DAG ID: postgres_metrics
# """
# import itertools

# import os
# from importlib.machinery import SourceFileLoader

# # from datetime import date, datetime, timedelta
# # from airflow.models import Variable
# from airflow.providers.google.cloud.operators.bigquery import (
#     BigQueryInsertJobOperator,
# )
# from airflow.operators.dummy_operator import DummyOperator

# metrics_path = f"{os.path.dirname(os.path.realpath(__file__))}/metrics"
# timeseries_pois = SourceFileLoader("timeseries_pois", f"{metrics_path}/timeseries_pois.py").load_module()


# def register(dag, start):  # sourcery skip: use-itertools-product
#     """
#     Register tasks on the dag.

#     Args:
#         dag (airflow.models.DAG): DAG instance to register tasks on.
#         start (airflow.models.TaskInstance): Task instance all tasks will
#             registered downstream from.

#     Returns:
#         airflow.models.TaskInstance: The last task node in this section.
#     """
#     # insert timeseries metrics into almanac_groups table for each grouping and granularity.
#     TIMESERIES_GROUPING_OPTIONS = ["fk_sgbrands", "top_category", "region"]

#     update_almanac_groups_end = DummyOperator(
#         task_id="update_almanac_groups_end",
#         dag=dag
#     )

#     clear_almanac_groups = BigQueryInsertJobOperator(
#         task_id="clear_almanac_groups",
#         configuration={
#             "query": {
#                 "query": "{% include './bigquery/clear_almanac_groups.sql' %}",
#                 "useLegacySql": "false",
#             }
#         }
#     )
#     update_almanac_pois_end >> clear_almanac_groups

#     prev_task = clear_almanac_groups
#     for granularity, group in itertools.product(TIMESERIES_GRANULARITY_OPTIONS,TIMESERIES_GROUPING_OPTIONS):

#         update_almanac_groups_correlation = BigQueryInsertJobOperator(
#             task_id=f"update_almanac_groups_{group}_correlation_{granularity}",
#             configuration={
#                 "query": {
#                     "query": "{% include './bigquery/get_name_temp_function.sql' %}"
#                             "{% include './bigquery/update_almanac_groups_correlation.sql' %}",
#                     "useLegacySql": "false",
#                 }
#             },
#             params={
#                 "granularity": granularity,
#                 "group_id": group,
#             }
#         )
#         prev_task >> update_almanac_groups_correlation

#         prev_task = update_almanac_groups_correlation

#     # insert GTVM metrics into almanac_groups table for each metric and granularity.
#     GTVM_METRIC_OPTIONS = ["kendall", "discrepancy", "divergence"]

#     for metric in GTVM_METRIC_OPTIONS:

#         if metric == "discrepancy":
#             query = "{% include './bigquery/update_almanac_groups_gtvm_discrepancy.sql' %}"
#         else:
#             query = "{% include './bigquery/update_almanac_groups_gtvm.sql' %}"

#         update_almanac_groups_gtvm = BigQueryInsertJobOperator(
#             task_id=f"update_almanac_groups_{metric}",
#             configuration={
#                 "query": {
#                     "query": "{% include './bigquery/get_name_temp_function.sql' %}"
#                             f"{query}",
#                     "useLegacySql": "false",
#                 }
#             },
#             params={
#                 "metric": metric,
#             }
#         )
#         prev_task >> update_almanac_groups_gtvm

#         prev_task = update_almanac_groups_gtvm

#     prev_task >> update_almanac_groups_end

#     return update_almanac_groups_end
