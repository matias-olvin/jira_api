import os
from importlib.machinery import SourceFileLoader

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.weight_rule import WeightRule
from common.utils import callbacks, dag_args, slack_users
from common.processes.triggers import trigger_sensor_group_w_timer

path = os.path.dirname(os.path.realpath(__file__))
DAG_ID = path.split("/")[-1]
steps_path = f"{path}/steps"

poi_feature_processing = SourceFileLoader(
    "poi_feature_processing",
    f"{steps_path}/groundtruth_volume_visits/poi_feature_processing.py",
).load_module()

train_ground_truth = SourceFileLoader(
    "train_ground_truth",
    f"{steps_path}/groundtruth_volume_visits/train_ground_truth.py",
).load_module()

get_metrics_block_groundtruth = SourceFileLoader(
    "get_metrics_block_groundtruth",
    f"{steps_path}/get_metrics_block_groundtruth.py",
).load_module()

sns_validation_block_groundtruth = SourceFileLoader(
    "sns_validation_block_groundtruth",
    f"{steps_path}/sns_validation_block_groundtruth.py",
).load_module()

env_args = dag_args.make_env_args()
default_args = dag_args.make_default_args(
    start_date=pendulum.datetime(2022, 9, 16, tz="Europe/London"),
    retries=0,
    weight_rule=WeightRule.UPSTREAM,
    on_failure_callback=callbacks.task_fail_slack_alert(
        slack_users.MATIAS, channel=env_args["env"]
    ),
    on_retry_callback=callbacks.task_retry_slack_alert(
        slack_users.MATIAS, channel=env_args["env"]
    ),
)


with DAG(
    DAG_ID,
    default_args=default_args,
    schedule_interval=env_args["schedule_interval"],
    tags=[env_args["env"], "smc", "externally triggered"],
    params=dag_args.load_config(scope="global"),
    doc_md=dag_args.load_docs(__file__),
    default_view="graph",
) as dag:
    start = EmptyOperator(task_id="start")

    groundtruth_preprocessing_end = poi_feature_processing.register(
        start=start, dag=dag
    )
    groundtruth_end = train_ground_truth.register(
        start=groundtruth_preprocessing_end, dag=dag
    )

    # POI scaling block 3 operations
    poi_visits_scaling_block_groundtruth_end = trigger_sensor_group_w_timer(
        start=groundtruth_end,
        trigger_dag_id="dag-processes-smc-block-groundtruth-scaling",
        poke_interval=60 * 10,
    )

    # Block 3 metrics
    block_groundtruth_metrics_end = get_metrics_block_groundtruth.register(
        start=poi_visits_scaling_block_groundtruth_end, dag=dag
    )

    block_groundtruth_sns_validation_end = sns_validation_block_groundtruth.register(
        start=block_groundtruth_metrics_end, dag=dag, env=env_args["env"]
    )

    end = EmptyOperator(
        task_id="end",
        on_success_callback=callbacks.pipeline_end_slack_alert(
            "U03BANPLXJR",
        ),
    )

    block_groundtruth_sns_validation_end >> end
