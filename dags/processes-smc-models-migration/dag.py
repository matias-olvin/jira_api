import os
from importlib.machinery import SourceFileLoader

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from common.utils import callbacks, dag_args

path = f"{os.path.dirname(os.path.realpath(__file__))}"
DAG_ID = path.split("/")[-1]
steps_path = f"{path}/steps"

migrate_models_task = SourceFileLoader(
    'migrate_models', f"{steps_path}/models_migration_task.py"
).load_module()

migrate_tables_task = SourceFileLoader(
    'migrate_tables', f"{steps_path}/tables_migration_task.py"
).load_module()

sns_task = SourceFileLoader(
    'sns_tasks', f"{steps_path}/sns_trigger.py"
).load_module()

env_args = dag_args.make_env_args()

default_args = dag_args.make_default_args(
    start_date=pendulum.datetime(2022, 3, 1, tz="Europe/London"),
    retries=0,
    on_retry_callback=callbacks.task_retry_slack_alert(
        "U05M60N8DMX",
        channel=env_args["env"]
    ),
    on_failure_callback=callbacks.task_fail_slack_alert(
        "U05M60N8DMX",
        channel=env_args["env"]
    )
)

with DAG(
    DAG_ID,
    params=dag_args.load_config(scope="global"),
    default_args=default_args,
    description="Parent DAG: scaling_models_creation_trigger, migrate models",
    schedule_interval=env_args["schedule_interval"],
    tags=[env_args["env"], "smc", "externally_triggered"],
    doc_md=dag_args.load_docs(__file__),
) as dag:
    start = EmptyOperator(task_id="start")

    end = EmptyOperator(task_id="end")

    tables_to_migrate_list = [
        "quality_dataset.model_input_complete_table",
        "visits_share_dataset.prior_distance_parameters_table",
        "daily_estimation_dataset.poi_list_table",
        "daily_estimation_dataset.grouped_daily_olvin_table",
        "daily_estimation_dataset.daily_factor_table",
        "ground_truth_volume_dataset.ground_truth_model_factor_per_poi_table",
        "ground_truth_volume_dataset.gtvm_output_table",
        "ground_truth_volume_dataset.prior_brand_visits_table",
        "ground_truth_volume_dataset.prior_brand_visits_backup_table",
    ]

    models_to_migrate_list = [
        "geoscaling_dataset.geoscaling_model",
        "visits_share_dataset.visits_share_model"
        ]

    models_migration_tasks = migrate_models_task.register(start=start, dag=dag, items_to_migrate=models_to_migrate_list)

    migrate_tables_tasks = migrate_tables_task.register(start=start, dag=dag, items_to_migrate=tables_to_migrate_list)

    sns_tasks = sns_task.register(start=start, dag=dag)

    [migrate_tables_tasks, sns_tasks, models_migration_tasks] >> end