import os
from datetime import datetime
from importlib.machinery import SourceFileLoader

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.weight_rule import WeightRule
from common.utils import callbacks, dag_args, slack_users
from common.processes.triggers import trigger_sensor_group_w_timer

path = os.path.dirname(os.path.realpath(__file__))
DAG_ID = path.split("/")[-1]
steps_path = f"{path}/steps"


get_metrics_block_0 = SourceFileLoader(
    "get_metrics_block_0", f"{steps_path}/block_0/get_metrics_block_0.py"
).load_module()

sns_validation_block_0 = SourceFileLoader(
    "sns_validation_block_0", f"{steps_path}/block_0/sns_validation_block_0.py"
).load_module()

naics_filtering = SourceFileLoader(
    "naics_filtering", f"{steps_path}/naics_filtering.py"
).load_module()

update_day_stats_visits_scaled = SourceFileLoader(
    "update_day_stats_visits_scaled", f"{steps_path}/update_day_stats_visits_scaled.py"
).load_module()

update_day_stats_visits = SourceFileLoader(
    "update_day_stats_visits", f"{steps_path}/update_day_stats_visits.py"
).load_module()


env_args = dag_args.make_env_args()
default_args = dag_args.make_default_args(
    start_date=datetime(2022, 9, 16),
    retries=0,
    weight_rule=WeightRule.UPSTREAM,
    on_failure_callback=callbacks.task_fail_slack_alert(slack_users.MATIAS),
    on_retry_callback=callbacks.task_retry_slack_alert(slack_users.MATIAS),
)


with DAG(
    DAG_ID,
    default_args=default_args,
    schedule_interval=env_args["schedule_interval"],
    tags=[env_args["env"], "smc", "externally_triggered"],
    params=dag_args.load_config(scope="global"),
    doc_md=dag_args.load_docs(__file__),
    default_view="graph",
) as dag:
    start = EmptyOperator(task_id="start")

    # BLOCK 0
    block_0_metrics_end = get_metrics_block_0.register(start=start, dag=dag)

    block_0_sns_validation_end = sns_validation_block_0.register(
        start=block_0_metrics_end, dag=dag, env=env_args["env"]
    )

    # UPDATE DAY STATS
    update_day_stats_visits_end = update_day_stats_visits.register(
        start=block_0_sns_validation_end, dag=dag
    )

    

    # BLOCK 1

    block_1_trigger_sensor_group = trigger_sensor_group_w_timer(
        start=update_day_stats_visits_end,
        trigger_dag_id="dag-processes-smc-block1",
        poke_interval=60 * 15,
    )

    # BLOCK DAILY ESTIMATION

    block_daily_estimation_trigger_sensor_group = trigger_sensor_group_w_timer(
        start=block_1_trigger_sensor_group,
        trigger_dag_id="dag-processes-smc-block-daily-estimation",
        poke_interval=60 * 10,
    )

    # Naics filtering

    naics_filtering_end = naics_filtering.register(
        start=block_daily_estimation_trigger_sensor_group, dag=dag
    )

    # BLOCK GTVM

    block_gtvm_trigger_sensor_group = trigger_sensor_group_w_timer(
        start=naics_filtering_end,
        trigger_dag_id="dag-processes-smc-block-groundtruth",
        poke_interval=60 * 10,
    )

    # UPDATE DAY STATS SCALED
    update_day_stats_visits_scaled_end = update_day_stats_visits_scaled.register(
        start=block_gtvm_trigger_sensor_group, dag=dag
    )

    end = EmptyOperator(
        task_id="end",
        on_success_callback=callbacks.pipeline_end_slack_alert(
            slack_users.MATIAS, channel=env_args["env"]
        ),
    )

    update_day_stats_visits_scaled_end >> end
