"""
DEPRECATED: 2024-01-01
"""
# """
# DAG ID: postgres_metrics
# """
# import os
# from datetime import datetime
# from importlib.machinery import SourceFileLoader

# from common.utils import dag_args, callbacks
# from airflow import DAG
# from airflow.operators.dummy_operator import DummyOperator

# path = f"{os.path.dirname(os.path.realpath(__file__))}"
# DAG_ID = path.split("/")[-1]
# steps_path = f"{path}/steps"
# env_args = dag_args.make_env_args(
#     dag_id=DAG_ID,
# )

# sns_poi_metrics = SourceFileLoader(
#     "sns_poi_metrics", f"{steps_path}/sns_poi_metrics.py"
# ).load_module()
# sns_group_metrics = SourceFileLoader(
#     "sns_group_metrics", f"{steps_path}/sns_group_metrics.py"
# ).load_module()

# with DAG(
#     env_args["dag_id"]+"-metrics",
#     default_args=dag_args.make_default_args(
#         start_date=datetime(2022, 1, 1),
#         retries=0,
#     ),
#     schedule_interval=env_args["schedule_interval"],
#     tags=[env_args["env"], "monthly"],
#     params=dag_args.load_config(__file__),
#     doc_md=dag_args.load_docs(__file__),
#     on_failure_callback=callbacks.task_fail_slack_alert(
#         #     "UPFLXHYHK", # Alfonso
#         #     "U02KJ556S1H", # Kartikey
#         "U034YDXAD1R",  # Jake
#     ),
# ) as metrics_dag:
#     start = DummyOperator(task_id="start")
#     sns_poi_metrics_end = sns_poi_metrics.register(metrics_dag, start)
#     sns_group_metrics_end = sns_group_metrics.register(metrics_dag, sns_poi_metrics_end)
#     end = DummyOperator(task_id="end")
#     sns_group_metrics_end >> end


# with DAG(
#     env_args["dag_id"]+"-metrics-validation",
#     default_args=dag_args.make_default_args(
#         start_date=datetime(2022, 1, 1),
#         retries=0,
#     ),
#     schedule_interval=env_args["schedule_interval"],
#     tags=[env_args["env"], "monthly"],
#     params=dag_args.load_config(__file__),
#     doc_md=dag_args.load_docs(__file__),
#     on_failure_callback=callbacks.task_fail_slack_alert(
#         #     "UPFLXHYHK", # Alfonso
#         #     "U02KJ556S1H", # Kartikey
#         "U034YDXAD1R",  # Jake
#     ),
# ) as metrics_validation_dag:
#     start = DummyOperator(task_id="start")
#     sns_poi_metrics_end = sns_poi_metrics.register(metrics_validation_dag, start)
#     sns_group_metrics_end = sns_group_metrics.register(
#         metrics_validation_dag, sns_poi_metrics_end
#     )
#     end = DummyOperator(task_id="end")
#     sns_group_metrics_end >> end
