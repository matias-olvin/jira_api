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

process_daily_estimation = SourceFileLoader(
    "process_daily_estimation", f"{steps_path}/process_daily_estimation.py"
).load_module()
get_metrics_block_daily_estimation = SourceFileLoader(
    "get_metrics_block_daily_estimation",
    f"{steps_path}/get_metrics_block_daily_estimation.py",
).load_module()
sns_validation_block_daily_estimation = SourceFileLoader(
    "sns_validation_block_daily_estimation",
    f"{steps_path}/sns_validation_block_daily_estimation.py",
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
    schedule_interval=None,
    tags=[env_args["env"], "smc", "externally triggered"],
    params=dag_args.load_config(scope="global"),
    doc_md=dag_args.load_docs(__file__),
    default_view="graph",
) as dag:
    start = EmptyOperator(task_id="start", dag=dag)

    # Daily estimation operations
    daily_estimation_processing_end = process_daily_estimation.register(
        start=start, dag=dag
    )

    # POI scaling block Daily estimation operations
    poi_visits_scaling_block_daily_estimation_end = trigger_sensor_group_w_timer(
        start=daily_estimation_processing_end,
        trigger_dag_id="dag-processes-smc-block-daily-estimation-scaling",
        poke_interval=60 * 10,
    )

    # Block Daily estimation metrics
    block_daily_estimation_metrics_end = get_metrics_block_daily_estimation.register(
        start=poi_visits_scaling_block_daily_estimation_end, dag=dag
    )

    # Block Daily estimation sns
    block_daily_estimation_sns_validation_end = (
        sns_validation_block_daily_estimation.register(
            start=block_daily_estimation_metrics_end, dag=dag, env=env_args["env"]
        )
    )

    end = EmptyOperator(
        task_id="end",
        on_success_callback=callbacks.pipeline_end_slack_alert(
            "U03BANPLXJR",
        ),
    )

    block_daily_estimation_sns_validation_end >> end
