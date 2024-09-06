"""
DAG ID: visits_daily
"""
import os
from datetime import datetime, timedelta
from importlib.machinery import SourceFileLoader

from common.utils import dag_args, callbacks, slack_users
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

# import places modules
path = f"{os.path.dirname(os.path.realpath(__file__))}"
DAG_ID = path.split("/")[-1]
places_steps_path = f"{path}/steps/places"

device_clusters = SourceFileLoader(
    "device_clusters", f"{places_steps_path}/device_clusters.py"
).load_module()
device_geohits = SourceFileLoader(
    "device_geohits", f"{places_steps_path}/device_geohits.py"
).load_module()
import_raw_tamoco = SourceFileLoader(
    "import_raw_tamoco", f"{places_steps_path}/import_raw_tamoco.py"
).load_module()
poi_visits_scaled = SourceFileLoader(
    "poi_visits_scaled", f"{places_steps_path}/poi_visits_scaled.py"
).load_module()
poi_visits = SourceFileLoader(
    "poi_visits", f"{places_steps_path}/poi_visits.py"
).load_module()
poi_stats_tables = SourceFileLoader(
    "poi_stats_tables", f"{places_steps_path}/stats_tables.py"
).load_module()
monitor_daily_stats_poi = SourceFileLoader(
    "monitor_daily_stats_poi", f"{places_steps_path}/monitoring/monitor_daily_stats.py"
).load_module()
test_daily_stats_poi = SourceFileLoader(
    "test_daily_stats_poi", f"{places_steps_path}/tests/test_daily_stats.py"
).load_module()
trigger_bloomberg_feed = SourceFileLoader(
    "trigger_bloomberg_feed", f"{places_steps_path}/trigger_bloomberg_feed.py"
).load_module()

# import trigger module
steps_path = f"{os.path.dirname(os.path.realpath(__file__))}/steps"
trigger_backfill = SourceFileLoader(
    "trigger_backfill", f"{steps_path}/trigger_backfill.py"
).load_module()

env_args = dag_args.make_env_args(
    dag_id=DAG_ID,
    schedule_interval="0 7 * * *",
)

with DAG(
    env_args["dag_id"],
    default_args=dag_args.make_default_args(
        start_date=datetime(2022, 9, 16),
        retries=3,
        depends_on_past=True,
        wait_for_downstream=True,
        on_failure_callback=callbacks.task_fail_slack_alert(
            slack_users.MATIAS,
        ),
    ),
    schedule_interval=env_args["schedule_interval"],
    tags=[env_args["env"], "daily"],
    catchup=True,
    dagrun_timeout=timedelta(hours=24),
    params=dag_args.load_config(__file__),
    doc_md=dag_args.load_docs(__file__),
) as update_dag:
    mode = "update"
    # Dummy start Task
    start = DummyOperator(task_id="start", dag=update_dag)

    # Pre-SMC Tasks
    import_raw_tamoco_end = import_raw_tamoco.register(
        dag=update_dag, start=start
    )  # [X] backfill compatible
    device_geohits_end = device_geohits.register(
        dag=update_dag, start=import_raw_tamoco_end, mode=mode
    )  # [X] backfill compatible
    device_clusters_end = device_clusters.register(
        dag=update_dag, start=device_geohits_end, mode=mode
    )  # [X] backfill compatible

    # Post-SMC Places Tasks
    poi_visits_end = poi_visits.register(
        update_dag, device_clusters_end
    )  # [X] backfill compatible
    trigger_bloomberg_feed_end = trigger_bloomberg_feed.register(
        update_dag, poi_visits_end
    )
    poi_stats_tables_end = poi_stats_tables.register(
        update_dag, poi_visits_end
    )  # [X] backfill compatible
    poi_visits_scaled_end = poi_visits_scaled.register(
        update_dag, poi_stats_tables_end
    )  # [X] backfill compatible
    monitor_daily_stats_poi_end = monitor_daily_stats_poi.register(
        update_dag, poi_visits_scaled_end
    )  # [X] backfill compatible
    test_daily_stats_poi_end = test_daily_stats_poi.register(
        dag=update_dag, start=poi_visits_scaled_end
    )  # [X] backfill compatible

with DAG(
    env_args["dag_id"]+"-post-aws-backfill",
    default_args=dag_args.make_default_args(
        start_date=datetime(2022, 9, 16),
        retries=3,
        depends_on_past=True,
        wait_for_downstream=True,
        on_failure_callback=callbacks.task_fail_slack_alert(
            slack_users.MATIAS
        ),
    ),
    schedule_interval=None,
    tags=[env_args["env"], "daily", "backfill"],
    params=dag_args.load_config(__file__),
    doc_md=dag_args.load_docs(__file__),
) as post_aws_backfill_dag:
    mode = "backfill"
    # Dummy start Task
    start = DummyOperator(task_id="start", dag=post_aws_backfill_dag)

    # Pre-SMC Tasks
    import_raw_tamoco_end = import_raw_tamoco.register(
        dag=post_aws_backfill_dag, start=start
    )  # [X] backfill compatible
    device_geohits_end = device_geohits.register(
        dag=post_aws_backfill_dag, start=import_raw_tamoco_end, mode=mode
    )  # [X] backfill compatible
    device_clusters_end = device_clusters.register(
        dag=post_aws_backfill_dag, start=device_geohits_end, mode=mode
    )  # [X] backfill compatible

    # Post-SMC Places Tasks
    poi_visits_end = poi_visits.register(
        post_aws_backfill_dag, device_clusters_end
    )  # [X] backfill compatible
    trigger_bloomberg_feed_end = trigger_bloomberg_feed.register(
        post_aws_backfill_dag, poi_visits_end
    )
    poi_stats_tables_end = poi_stats_tables.register(
        post_aws_backfill_dag, poi_visits_end
    )  # [X] backfill compatible
    poi_visits_scaled_end = poi_visits_scaled.register(
        post_aws_backfill_dag, poi_stats_tables_end
    )  # [X] backfill compatible
    monitor_daily_stats_poi_end = monitor_daily_stats_poi.register(
        post_aws_backfill_dag, poi_visits_scaled_end
    )  # [X] backfill compatible
    test_daily_stats_poi_end = test_daily_stats_poi.register(
        dag=post_aws_backfill_dag, start=poi_visits_scaled_end
    )  # [X] backfill compatible

with DAG(
    env_args["dag_id"]+"-post-cluster-backfill",
    default_args=dag_args.make_default_args(
        start_date=datetime(2022, 9, 16),
        retries=3,
        depends_on_past=True,
        wait_for_downstream=True,
        on_failure_callback=callbacks.task_fail_slack_alert(slack_users.MATIAS),
    ),
    schedule_interval=None,
    tags=[env_args["env"], "daily", "backfill"],
    params=dag_args.load_config(__file__),
    doc_md=dag_args.load_docs(__file__),
) as post_cluster_backfill_dag:
    # parameter to determine whether update or backfill ('update' is default value)
    mode = "backfill"

    # Dummy start Task
    start = DummyOperator(task_id="start", dag=post_cluster_backfill_dag)

    # Post-SMC Places Tasks
    poi_visits_end = poi_visits.register(
        dag=post_cluster_backfill_dag, start=start
    )  # [X] smc backfill compatible
    poi_stats_tables_end = poi_stats_tables.register(
        dag=post_cluster_backfill_dag, start=poi_visits_end, mode=mode
    )  # [X] smc backfill compatible
    poi_visits_scaled_end = poi_visits_scaled.register(
        dag=post_cluster_backfill_dag, start=poi_stats_tables_end
    )  # [X] smc backfill compatible
    monitor_daily_stats_poi_end = monitor_daily_stats_poi.register(
        dag=post_cluster_backfill_dag, start=poi_visits_scaled_end
    )  # [X] smc backfill compatible

with DAG(
    env_args["dag_id"]+"-post-cluster-backfill-trigger",
    default_args=dag_args.make_default_args(
        start_date=datetime(2022, 9, 16),
        retries=3,
        depends_on_past=True,
        wait_for_downstream=True,
        on_failure_callback=callbacks.task_fail_slack_alert(
            slack_users.MATIAS,
        ),
    ),
    schedule_interval=None,
    tags=[env_args["env"], "daily", "backfill"],
    params=dag_args.load_config(__file__),
    doc_md=dag_args.load_docs(__file__),
    on_success_callback=callbacks.pipeline_end_slack_alert(
        "U034YDXAD1R", "U02KJ556S1H"  # Jake  # Kartikey
    ),
) as trigger_post_cluster_backfill_dag:
    start = DummyOperator(task_id="start", dag=trigger_post_cluster_backfill_dag)
    trigger_backfill_end = trigger_backfill.register(
        dag=trigger_post_cluster_backfill_dag,
        start=start,
        pipeline=trigger_post_cluster_backfill_dag.params[
            "visits_all_daily_smc_backfill_pipeline"
        ],
    )

with DAG(
    env_args["dag_id"]+"-post-aws-backfill-trigger",
    default_args=dag_args.make_default_args(
        start_date=datetime(2022, 9, 16),
        retries=3,
        depends_on_past=True,
        wait_for_downstream=True,
        on_failure_callback=callbacks.task_fail_slack_alert(
            slack_users.MATIAS,
        ),
    ),
    schedule_interval=None,
    tags=[env_args["env"], "daily", "backfill"],
    params=dag_args.load_config(__file__),
    doc_md=dag_args.load_docs(__file__),
    on_success_callback=callbacks.pipeline_end_slack_alert(
        slack_users.MATIAS,
    ),
) as trigger_post_aws_backfill_dag:
    start = DummyOperator(task_id="start", dag=trigger_post_aws_backfill_dag)
    trigger_backfill_end = trigger_backfill.register(
        dag=trigger_post_aws_backfill_dag,
        start=start,
        pipeline=trigger_post_aws_backfill_dag.params[
            "visits_all_daily_raw_backfill_pipeline"
        ],
    )
