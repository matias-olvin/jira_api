# """
# DAG ID: daily_pipeline_metadata
# """
# from airflow.sensors.external_task import (
#     ExternalTaskSensor,
# )
# from airflow.operators.python_operator import (
#     PythonOperator,
# )
# from airflow.providers.google.cloud.operators.bigquery import (
#     BigQueryInsertJobOperator,
# )
# from airflow.operators.dummy_operator import (
#     DummyOperator,
# )
# from airflow.models import (
#     Variable,
#     TaskInstance
# )
# from airflow import DAG

# from kpi_pipeline.functions.dag_run_metadata import (
#     dag_metadata,
# )
# from kpi_pipeline.functions.bq_metadata import (
#     extract_pipeline_tables,
#     extract_storage_bytes,
# )
# from kpi_pipeline.functions.gcp_billing_report import (
#     extract_billing_info,
# )


# def register(dag: DAG, start: TaskInstance, pipelines: list, run_type: str) -> TaskInstance:
#     """
#     Register tasks on the dag

#     Args:
#         dag (airflow.models.DAG): DAG instance to register tasks on.
#         start (airflow.models.TaskInstance): Task instance all tasks will
#         be registered downstream from.

#     Returns:
#         airflow.models.TaskInstance: The last task node in this section.
#     """
#     from datetime import timedelta

#     table_id = dag.params['pipeline_metadata_table']
#     run_date = f"'{{{{ ds }}}}'"

#     for pipeline in pipelines:

#         other = False
#         # create pipeline_id as shorthand for pipelines
#         pipeline_label = pipeline.split('_')[0] + '_' + pipeline.split('_')[-1]
#         if pipeline == "other":
#             other = True
#             pipeline_label = "other"
#         # set parameters for each run_type
#         if run_type == 'daily':
#             # get execution delta for pipeline
#             if pipeline ==  "weather_pipeline_historical":
#                 execution_delta = timedelta(minutes=30)
#             elif pipeline == "regressors_collection_predicthq_events":
#                 execution_delta = timedelta(hours=2)
#             else:
#                 execution_delta = timedelta(hours=5)
#             timeout = 12*60*60
#             poke_interval = 60*60
#             date_filter = f"DATE(_PARTITIONTIME) = CAST({run_date} AS DATE)"

#         elif run_type == 'weekly':
#             if pipeline == "regressors_collection_covid":
#                 execution_delta = timedelta(days=4, hours=8)
#             elif pipeline == "regressors_collection_group_visits":
#                 execution_delta = timedelta(days=1)
#             date_filter = f""" (
#                 DATE(_PARTITIONTIME) > DATE_SUB({run_date}, INTERVAL 7 DAY)) AND
#                 DATE(_PARTITIONTIME) <= CAST({run_date} AS DATE
#             ) """
#             timeout = 3*24*60*60
#             poke_interval = 3*60*60

#         elif run_type == 'monthly':
#             if pipeline == "dynamic_places":
#                 execution_delta = timedelta(days=3)
#                 run_date = "DATE_SUB('{{ ds }}', INTERVAL 3 DAY)"
#             else:
#                 execution_delta = timedelta(days=5)
#                 run_date = "DATE_SUB('{{ ds }}', INTERVAL 5 DAY)"
#             date_filter = f""" (
#                 DATE(_PARTITIONTIME) > LAST_DAY(DATE_SUB({run_date}, INTERVAL 1 MONTH)) AND
#                 DATE(_PARTITIONTIME) <= LAST_DAY(CAST({run_date} AS DATE))
#             ) """
#             timeout = 10*24*60*60
#             poke_interval = 24*60*60


#         elif run_type == 'quarterly':
#             # if pipeline == "dynamic_places":
#             #     execution_delta = timedelta(days=3)
#             #     run_date = "DATE_SUB('{{ ds }}', INTERVAL 3 DAY)"
#             # else:
#             #     execution_delta = timedelta(days=5)
#             #     run_date = "DATE_SUB('{{ ds }}', INTERVAL 5 DAY)"
#             execution_delta = timedelta(days=0)
#             date_filter = f""" (
#                 DATE(_PARTITIONTIME) > LAST_DAY(DATE_SUB({run_date}, INTERVAL 3 MONTH)) AND
#                 DATE(_PARTITIONTIME) <= LAST_DAY(CAST({run_date} AS DATE))
#             ) """
#             timeout = 10*24*60*60
#             poke_interval = 24*60*60

#         # elif run_type == 'triggered':
#         #     # table_id = run_type + dag.params['pipeline_metadata_table']
#         #     date_filter = "EXTRACT(MONTH FROM DATE(creation_time)) = EXTRACT(MONTH FROM DATE_ADD('{{ ds }}', INTERVAL 1 MONTH))"
#         #     start_col = "start_date"
#         #     end_col = "end_date"

#         # wait for pipelines to complete run
#         if run_type != 'triggered':
#             if not other:
#                 wait_for_pipeline = ExternalTaskSensor(
#                     task_id='wait_for_' + pipeline_label,
#                     external_dag_id=pipeline,
#                     external_task_id=None,
#                     allowed_states=[
#                         "success"
#                     ],
#                     execution_delta=execution_delta,
#                     check_existence=True,
#                     poke_interval=poke_interval,
#                     timeout=timeout,
#                     soft_fail=False,
#                     mode="reschedule",
#                     dag=dag
#                 )

#                 # get metadata from DAGRUN
#                 get_latest_metadata = PythonOperator(
#                     task_id="get_metadata_" + pipeline_label,
#                     provide_context=True,
#                     project_id=Variable.get("compute_project_id"),
#                     python_callable=dag_metadata,
#                     op_kwargs={
#                         "pipeline": pipeline,
#                         "run_type": run_type
#                     },
#                     dag=dag,
#                     location="EU",
#                 )

#                 # append metadata to `storage-prod-olvin-com.test_compilation.daily_pipeline_metadata`
#                 insert_latest_metadata_bq = BigQueryInsertJobOperator(
#                     task_id=f"insert_latest_metadata_bq_" + pipeline_label,
#                     project_id=Variable.get("compute_project_id"),
#                     configuration={
#                         "query": {
#                             "query": f"""
#                             INSERT INTO `{{{{ params['project_id'] }}}}.{{{{ params['pipeline_tracking_dataset'] }}}}.{table_id}`
#                             (
#                                 run_date,
#                                 pipeline,
#                                 start_time,
#                                 end_time,
#                                 state,
#                                 duration,
#                                 schedule
#                             )
#                             VALUES
#                                 {{{{ ti.xcom_pull(key='rows_to_insert', task_ids='get_metadata_{pipeline_label}') }}}}
#                             ;""",
#                             "useLegacySql": "False",
#                         },
#                         "labels": {
#                             "pipeline":f"{run_type}_metadata"
#                         },
#                     },
#                     dag=dag,
#                     location="EU",
#                 )

#             else:
#                 insert_new_row_other = BigQueryInsertJobOperator(
#                     task_id=f"insert_new_row_other",
#                     project_id=Variable.get("compute_project_id"),
#                     configuration={
#                         "query": {
#                             "query": f"""
#                             INSERT INTO `{{{{ params['project_id'] }}}}.{{{{ params['pipeline_tracking_dataset'] }}}}.{table_id}`
#                             (
#                                 run_date,
#                                 pipeline,
#                                 schedule
#                             )
#                             VALUES
#                                 (
#                                     {run_date},
#                                     '{pipeline}',
#                                     '{run_type}'
#                                 )
#                             ;""",
#                             "useLegacySql": "False",
#                         },
#                         "labels": {
#                             "pipeline":f"{run_type}_metadata"
#                         },
#                     },
#                     dag=dag,
#                     location="EU",
#                 )


#         # query gcp_billing_report in storage-prod-olvin-com for
#         # bytes and costs billed for each pipeline and append results to
#         # `storage-prod-olvin-com.test_compilation.daily_pipeline_metadata`
#         extract_gcp_billing_info = PythonOperator(
#             task_id='extract_gcp_billing_info_' + pipeline_label,
#             provide_context=True,
#             project_id=Variable.get("compute_project_id"),
#             python_callable=extract_billing_info,
#             op_kwargs={
#                 "dag": dag,
#                 "pipeline": pipeline,
#                 "pipeline_label": pipeline_label,
#                 "run_date": run_date,
#                 "date_filter": date_filter
#             },
#             dag=dag,
#             location="EU",
#         )

#         # end
#         end = DummyOperator(
#             task_id='end',
#             dag=dag,
#         )

#         # Wire tasks
#         if run_type != "triggered":
#             if not other:
#                 (
#                     start
#                     >> wait_for_pipeline
#                     >> get_latest_metadata
#                     >> insert_latest_metadata_bq
#                     >> extract_gcp_billing_info
#                     >> end
#                 )
#             else:
#                 (
#                     start
#                     >> insert_new_row_other
#                     >> extract_gcp_billing_info
#                     >> end
#                 )
#         else:
#             (
#                 start
#                 >> extract_gcp_billing_info
#                 >> end
#             )

#     return end
