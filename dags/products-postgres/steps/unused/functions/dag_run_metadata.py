# from airflow.models.dagbag import DagBag
# from airflow.models.dagrun import DagRun


# def dag_metadata(pipeline: str, run_type: str, **kwargs):   # -> pushes result to XCom
#     """
#     Get startime and endtime for DAG
#     """
#     row_to_insert = ""
#     # use dagbag to get execution date for latest DagRun
#     dag = DagBag().get_dag(dag_id=pipeline)
#     last_dagrun_run_id = dag.get_last_dagrun(include_externally_triggered=True)
#     # get list of all DagRuns
#     dag_runs = DagRun.find(dag_id=pipeline)
#     for dag_run in dag_runs:
#         if dag_run.execution_date == last_dagrun_run_id.execution_date:
#             # get metadata
#             execution_date = dag_run.execution_date
#             start_time = dag_run.start_date
#             end_time = dag_run.end_date
#             state = dag_run.state

#     # calculate run time in seconds
#     run_time = end_time - start_time
#     duration = run_time.total_seconds()

#     # if run_type == 'daily':
#     row_to_insert += f"""(
#         CAST('{execution_date.strftime('%Y-%m-%d')}' AS DATE),
#         '{pipeline}',
#         CAST('{start_time.time()}' AS TIME),
#         CAST('{end_time.time()}' AS TIME),
#         '{state}',
#         CAST('{duration}' AS NUMERIC),
#         '{run_type}'
#     )"""

#     # elif run_type == 'monthly':
#     #     row_to_insert += f"""(
#     #         CAST('{execution_date.strftime('%Y-%m-%d')}' AS DATE),
#     #         '{pipeline}',
#     #         CAST('{start_time.strftime('%Y-%m-%d %H:%M:%S')}' AS DATETIME),
#     #         CAST('{end_time.strftime('%Y-%m-%d %H:%M:%S')}' AS DATETIME),
#     #         '{state}',
#     #         CAST('{duration}' AS NUMERIC)
#     #     )"""

#     # Push to XCom
#     kwargs['ti'].xcom_push(key='rows_to_insert', value=row_to_insert)
