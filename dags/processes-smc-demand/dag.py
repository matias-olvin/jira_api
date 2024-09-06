import os
from datetime import datetime
from importlib.machinery import SourceFileLoader

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.weight_rule import WeightRule
from common.utils import callbacks, dag_args, slack_users

path = os.path.dirname(os.path.realpath(__file__))
DAG_ID = path.split("/")[-1]
steps_path = f"{path}/steps"

# PRE-SMC

set_date = SourceFileLoader(
        "set_date", f"{steps_path}/pre_smc/set_execution_date.py"
    ).load_module()

pre_smc_demand = SourceFileLoader(
    "pre_smc_demand", f"{steps_path}/pre_smc/pre_smc_demand.py"
).load_module()

# SMC
smc_poi_visits_scaling_block_1 = SourceFileLoader(
    "smc_poi_visits_scaling_block_1", f"{steps_path}/smc/poi_visits_scaling_block_1.py"
).load_module()

smc_poi_visits_scaling_block_daily_estimation = SourceFileLoader(
    "smc_poi_visits_scaling_block_daily_estimation", f"{steps_path}/smc/poi_visits_scaling_block_daily_estimation.py"
).load_module()

smc_poi_visits_scaling_block_groundtruth = SourceFileLoader(
    "smc_poi_visits_scaling_block_groundtruth", f"{steps_path}/smc/poi_visits_scaling_block_groundtruth.py"
).load_module()

smc_naics_filtering = SourceFileLoader(
    "smc_naics_filtering", f"{steps_path}/smc/naics_filtering.py"
).load_module()

smc_update_day_stats_visits_scaled = SourceFileLoader(
    "smc_update_day_stats_visits_scaled", f"{steps_path}/smc/update_day_stats_visits_scaled.py"
).load_module()

set_current_date = SourceFileLoader(
    "set_current_date_task", f"{steps_path}/smc/set_current_date.py"
).load_module()

# POST-SMC
post_smc_poi_visits_scaling_block_1 = SourceFileLoader(
    "post_smc_poi_visits_scaling_block_1", f"{steps_path}/post_smc/poi_visits_scaling_block_1.py"
).load_module()


post_smc_poi_visits_scaling_block_daily_estimation = SourceFileLoader(
    "post_smc_poi_visits_scaling_block_daily_estimation", f"{steps_path}/post_smc/poi_visits_scaling_block_daily_estimation.py"
).load_module()

post_smc_poi_visits_scaling_block_groundtruth = SourceFileLoader(
    "post_smc_poi_visits_scaling_block_groundtruth", f"{steps_path}/post_smc/poi_visits_scaling_block_groundtruth.py"
).load_module()

post_smc_naics_filtering = SourceFileLoader(
    "post_smc_naics_filtering", f"{steps_path}/post_smc/naics_filtering.py"
).load_module()

post_smc_update_day_stats_visits_scaled = SourceFileLoader(
    "post_smc_update_day_stats_visits_scaled", f"{steps_path}/post_smc/update_day_stats_visits_scaled.py"
).load_module()

post_smc_demand = SourceFileLoader(
    "post_smc_demand", f"{steps_path}/post_smc/post_smc_demand.py"
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

    # PRE-SMC

    # SET_DATE
    set_date_end = set_date.register(start=start, dag=dag)

    pre_smc_demand_end = pre_smc_demand.register(
        start=set_date_end, dag=dag
    )

    # SMC

    # BLOCK 1

    smc_poi_visits_scaling_block_1_end = smc_poi_visits_scaling_block_1.register(
        start=pre_smc_demand_end, dag=dag
    )

    # BLOCK DAILY ESTIMATION

    smc_poi_visits_scaling_block_daily_estimation_end = smc_poi_visits_scaling_block_daily_estimation.register(
        start=smc_poi_visits_scaling_block_1_end, dag=dag
    )

    # Naics filtering

    smc_naics_filtering_end = smc_naics_filtering.register(
        start=smc_poi_visits_scaling_block_daily_estimation_end, dag=dag
    )

    # BLOCK GTVM

    smc_poi_visits_scaling_block_groundtruth_end = smc_poi_visits_scaling_block_groundtruth.register(
        start=smc_naics_filtering_end, dag=dag
    )

    # UPDATE DAY STATS SCALED
    smc_update_day_stats_visits_scaled_end = smc_update_day_stats_visits_scaled.register(
        start=smc_poi_visits_scaling_block_groundtruth_end, dag=dag
    )

    # SET CURRENT DATE
    set_current_date_task = set_current_date.register(
        start=smc_update_day_stats_visits_scaled_end, dag=dag
    )

    # POST-SMC
    
    # BLOCK 1

    post_smc_poi_visits_scaling_block_1_end = post_smc_poi_visits_scaling_block_1.register(
        start=set_current_date_task, dag=dag
    )

    # BLOCK DAILY ESTIMATION

    post_smc_poi_visits_scaling_block_daily_estimation_end = post_smc_poi_visits_scaling_block_daily_estimation.register(
        start=post_smc_poi_visits_scaling_block_1_end, dag=dag
    )

    # Naics filtering

    post_smc_naics_filtering_end = post_smc_naics_filtering.register(
        start=post_smc_poi_visits_scaling_block_daily_estimation_end, dag=dag
    )

    # BLOCK GTVM

    post_smc_poi_visits_scaling_block_groundtruth_end = post_smc_poi_visits_scaling_block_groundtruth.register(
        start=post_smc_naics_filtering_end, dag=dag
    )

    # UPDATE DAY STATS SCALED
    post_smc_update_day_stats_visits_scaled_end = post_smc_update_day_stats_visits_scaled.register(
        start=post_smc_poi_visits_scaling_block_groundtruth_end, dag=dag
    )

    post_smc_demand_end = post_smc_demand.register(
        start=post_smc_update_day_stats_visits_scaled_end, dag=dag
    )

    end = EmptyOperator(
        task_id="end",
        on_success_callback=callbacks.pipeline_end_slack_alert(
            "U03BANPLXJR",
        ),
    )

    post_smc_demand_end >> end
