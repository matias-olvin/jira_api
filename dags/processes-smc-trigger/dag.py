import os
from importlib.machinery import SourceFileLoader

from airflow import DAG
import pendulum
from airflow.operators.empty import EmptyOperator

from common.utils import dag_args, callbacks
from common.processes.triggers import trigger_sensor_group_w_timer

path = f"{os.path.dirname(os.path.realpath(__file__))}"
DAG_ID = path.split("/")[-1]
steps_path = f"{path}/steps"

# SET_DATE
set_date = SourceFileLoader(
    "set_date", f"{steps_path}/set_execution_date.py"
).load_module()
set_manually_add_pois_date = SourceFileLoader(
    "set_manually_add_pois_date", f"{steps_path}/set_manually_add_pois_date.py"
).load_module()
# SCALING MODELS CREATION
sg_agg_stats_backfill_tasks = SourceFileLoader(
    "sg_agg_stats_backfill_tasks", f"{steps_path}/smc/trigger_sg_agg_stats_backfill.py"
).load_module()
cameo_profiles_backfill_tasks = SourceFileLoader(
    "cameo_profiles_backfill_tasks", f"{steps_path}/smc/trigger_cameo_profiles_backfill.py"
).load_module()
# CREATE PLACEKEY
poi_matching_tasks = SourceFileLoader(
    "trigger_poi_matching", f"{steps_path}/smc/trigger_poi_matching.py"
).load_module()
set_current_date = SourceFileLoader(
    "set_current_date_task", f"{steps_path}/smc/set_current_date.py"
).load_module()

visits_daily_backfill_tasks = SourceFileLoader(
    "visits_daily_backfill_tasks", f"{steps_path}/smc/trigger_visits_daily_backfill.py"
).load_module()

notification = SourceFileLoader(
    "notification_task", f"{steps_path}/notification/notification_task.py"
).load_module()

trunctate_scaling = SourceFileLoader(
    "trunctate_scaling", f"{steps_path}/truncate_scaling_tables.py"
).load_module()

env_args = dag_args.make_env_args()

default_args = dag_args.make_default_args(
    start_date=pendulum.datetime(2022, 3, 1, tz="Europe/London"),
    retries=0,
    on_retry_callback=callbacks.task_retry_slack_alert(
        "U034YDXAD1R",
        channel=env_args["env"]
    ),
    on_failure_callback=callbacks.task_fail_slack_alert(
        "U034YDXAD1R",
        channel=env_args["env"]
    )
)

with DAG(
    DAG_ID,
    params=dag_args.load_config(scope="global"),
    default_args=default_args,
    description="scaling model creation dag",
    schedule_interval=env_args["schedule_interval"],
    tags=[env_args["env"], "smc", "externally_triggered"],
    doc_md=dag_args.load_docs(__file__),
) as dag:
    start = EmptyOperator(task_id="start")

    # SET DATE
    set_date_end = set_date.register(start=start, dag=dag)

    # TRIGGER PLACES_DYNAMIC
    places_dynamic_trigger_tasks = trigger_sensor_group_w_timer(
        start=set_date_end,
        trigger_dag_id="dag-processes-smc-sg-places-dynamic",
        poke_interval=60 * 5,
    )

    # TRIGGER SEND_TO_POSTGRES
    send_to_postgres_trigger_tasks = trigger_sensor_group_w_timer(
        start=places_dynamic_trigger_tasks,
        trigger_dag_id="dag-processes-smc-dynamic-postgres",
        poke_interval=60 * 5,
    )

    # TRIGGER REGRESSORS
    regressors_trigger_tasks = trigger_sensor_group_w_timer(
        start=places_dynamic_trigger_tasks,
        trigger_dag_id="dag-processes-smc-regressors-dynamic",
        poke_interval=60 * 5,
    )

    # POI VISITS BACKFILL
    poi_visits_backfill_tasks_end = trigger_sensor_group_w_timer(
        start=regressors_trigger_tasks,
        trigger_dag_id="poi_visits_backfill_pipeline",
        poke_interval=60 * 5,
        external_task_id="end_2024",
    )

    # POI MATCHING
    poi_matching_tasks_end = poi_matching_tasks.register(
        start=places_dynamic_trigger_tasks, dag=dag
    )

    # SMC
    smc_tasks_end = trigger_sensor_group_w_timer(
        start=[poi_matching_tasks_end, poi_visits_backfill_tasks_end, send_to_postgres_trigger_tasks],
        trigger_dag_id="scaling_models_creation_pipeline",
        poke_interval=60 * 30,
    )

    # SET CURRENT DATE
    set_current_date_task = set_current_date.register(
        start=smc_tasks_end, dag=dag
    )

    # VISITS DAILY BACKFILL
    visits_daily_backfill_tasks_end = visits_daily_backfill_tasks.register(
        start=set_current_date_task, dag=dag
    )

    # TRIGGER PLACES MIGRATION
    places_migration_trigger = trigger_sensor_group_w_timer(
        start=visits_daily_backfill_tasks_end,
        trigger_dag_id="dag-processes-smc-places-migration",
        poke_interval=60 * 5,
    )

    # SET MANUALLY ADD POIS DEADLINE DATE
    set_manually_add_pois_date_end = set_manually_add_pois_date.register(start=places_migration_trigger)

    # TRIGGER REGRESSORS MIGRATION
    regressors_migration_trigger = trigger_sensor_group_w_timer(
        start=set_manually_add_pois_date_end,
        trigger_dag_id="dag-processes-smc-regressors-migration",
        poke_interval=60 * 5,
    )

    # TRIGGER MODELS MIGRATION
    models_migration_trigger = trigger_sensor_group_w_timer(
        start=regressors_migration_trigger,
        trigger_dag_id="dag-processes-smc-models-migration",
        poke_interval=60 * 5,
    )

    # TRIGGER METRICS MIGRATION
    metrics_migration_trigger = trigger_sensor_group_w_timer(
        start=models_migration_trigger,
        trigger_dag_id="dag-processes-smc-metrics-migration",
        poke_interval=60 * 5,
    )

    # TRIGGER VISITS/VISITS_SCALED MIGRATION
    visits_migration_trigger = trigger_sensor_group_w_timer(
        start=metrics_migration_trigger,
        trigger_dag_id="dag-processes-smc-visits-migration",
        poke_interval=60 * 5,
    )

    # SG AGG STATS BACKFILL
    sg_agg_stats_backfill_tasks_end = sg_agg_stats_backfill_tasks.register(
        start=visits_migration_trigger, dag=dag
    )

    # CAMEO PROFILES BACKFILL
    cameo_profiles_backfill_tasks_end = cameo_profiles_backfill_tasks.register(
        start=sg_agg_stats_backfill_tasks_end, dag=dag
    )

    # TRUNCATE SCALING TABLES

    # values should be the keys for the corresponding value found in the global config file
    scaling_datasets = [
        "poi_visits_block_daily_estimation_dataset",
        "poi_visits_block_1_dataset",
    ]

    trunctate_scaling_tasks = trunctate_scaling.register(start=cameo_profiles_backfill_tasks_end, dag=dag, dataset_to_truncate=scaling_datasets)

    # ALERT ON COMPLETION
    notification_task = notification.register(
        start=trunctate_scaling_tasks, dag=dag
    )


