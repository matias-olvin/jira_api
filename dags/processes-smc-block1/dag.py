import os
from importlib.machinery import SourceFileLoader

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.weight_rule import WeightRule
from common.utils import callbacks, dag_args, slack_users
from common.processes.triggers import trigger_sensor_group_w_timer
from pendulum import datetime

path = os.path.dirname(os.path.realpath(__file__))
DAG_ID = path.split("/")[-1]
steps_path = f"{path}/steps"

# GEOSCALING MODEL
process_geoscaling_data = SourceFileLoader(
    "process_quality_data", f"{steps_path}/geoscaling/process_geoscaling_data.py"
).load_module()

# VISITS SHARE
classifier_prior_distances = SourceFileLoader(
    "classifier_prior_distances",
    f"{steps_path}/visits_share/classifier_prior_distances.py",
).load_module()
train_visits_share = SourceFileLoader(
    "train_visits_share", f"{steps_path}/visits_share/train_visits_share.py"
).load_module()

# METRICS
get_metrics_block_1 = SourceFileLoader(
    "get_metrics_block_1", f"{steps_path}/get_metrics_block_1.py"
).load_module()

# SNS
sns_validation_block_1 = SourceFileLoader(
    "sns_validation_block_1", f"{steps_path}/sns_validation_block_1.py"
).load_module()

env_args = dag_args.make_env_args()

default_args = dag_args.make_default_args(
    start_date=datetime(2022, 9, 16, tz="Europe/London"),
    depends_on_past=False,
    retries=0,
    weight_rule=WeightRule.UPSTREAM,
    on_failure_callback=callbacks.task_fail_slack_alert(slack_users.MATIAS),
    on_retry_callback=callbacks.task_retry_slack_alert(slack_users.MATIAS),
)


with DAG(
    DAG_ID,
    default_args=default_args,
    # concurrency=18,
    schedule_interval=None,
    tags=[env_args["env"], "smc", "externally triggered"],
    catchup=False,
    params=dag_args.load_config(scope="global"),
    doc_md=dag_args.load_docs(__file__),
    default_view="graph",
) as dag:
    start = EmptyOperator(task_id="start")

    # Geoscaling
    process_geoscaling_data_end = process_geoscaling_data.register(start=start, dag=dag)

    # Visits Share
    classifier_prior_distances_end = classifier_prior_distances.register(
        start=start, dag=dag
    )
    train_visits_share_end = train_visits_share.register(
        start=classifier_prior_distances_end, dag=dag
    )

    # POI scaling block 1 operations
    poi_visits_scaling_block_1_start = EmptyOperator(
        task_id="poi_visits_scaling_block_1_start"
    )
    [
        process_geoscaling_data_end,
        train_visits_share_end,
    ] >> poi_visits_scaling_block_1_start

    # VISITS SCALING TRIGGER SENSOR GROUP
    poi_visits_scaling_block_1_trigger = trigger_sensor_group_w_timer(
        start=poi_visits_scaling_block_1_start,
        trigger_dag_id="dag-processes-smc-block1-scaling",
        poke_interval=60 * 10,
    )

    block_1_metrics_end = get_metrics_block_1.register(
        start=poi_visits_scaling_block_1_trigger, dag=dag
    )

    block_1_sns_validation_end = sns_validation_block_1.register(
        start=block_1_metrics_end, dag=dag
    )

    end = EmptyOperator(task_id="end")

    block_1_sns_validation_end >> end
