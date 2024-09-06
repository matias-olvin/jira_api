"""
DAG ID: sg_agg_stats
"""
import os,pendulum
from datetime import datetime
from importlib.machinery import SourceFileLoader

from common.utils import dag_args, callbacks
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.weight_rule import WeightRule
from common.utils import slack_users

# Relative imports based on the current file location
path = f"{os.path.dirname(os.path.realpath(__file__))}"
steps_path = path + "/steps"

DAG_ID = path.split("/")[-1]

env_args = dag_args.make_env_args(
    dag_id=DAG_ID,
)

vhl_computation = SourceFileLoader(
    "vhl_computation", f"{steps_path}/vhl_computation.py"
).load_module()
activity_computation = SourceFileLoader(
    "activity_computation", f"{steps_path}/activity_computation.py"
).load_module()
activity_computation_postgres = SourceFileLoader(
    "activity_computation_postgres", f"{steps_path}/activity_computation_postgres.py"
).load_module()
export_results = SourceFileLoader(
    "export_results", f"{steps_path}/demographics_tests/export_results.py"
).load_module()
vhl_send = SourceFileLoader("vhl_send", f"{steps_path}/vhl_send.py").load_module()
update_sgplaceactivity_with_zipcode_yearly = SourceFileLoader(
    "update_sgplaceactivity_with_zipcode_yearly", f"{steps_path}/update_sgplaceactivity_with_zipcode_yearly.py"
).load_module()


# Define the dates for computation
def prev_month_formatted(execution_date):
    return execution_date.subtract(months=1).strftime("%Y-%m-01")


def run_month_formatted(execution_date):
    return execution_date.strftime("%Y-%m-01")


def next_run_month_formatted(execution_date):
    return execution_date.add(months=1).strftime("%Y-%m-01")


def first_month_formatted(_):
    return "2019-01-01"


def first_buffer_month_formatted(_):
    return "2019-04-01"


# Create monthly DAG
with DAG(
    env_args["dag_id"],
    default_args=dag_args.make_default_args(
    start_date=pendulum.datetime(2022, 7, 1, tz="Europe/London"),
    retries=0,
    on_failure_callback=callbacks.task_fail_slack_alert(
        slack_users.MATIAS,
        slack_users.CARLOS,
    ),
    on_retry_callback=callbacks.task_retry_slack_alert(
        slack_users.MATIAS,
        slack_users.CARLOS,
    ),
    weight_rule=WeightRule.UPSTREAM,
),
    concurrency=4,
    schedule_interval=env_args["schedule_interval"],
    tags=[env_args["env"], "monthly"],
    params=dag_args.load_config(__file__),
    # doc_md=dag_args.load_docs(__file__),
    user_defined_macros={
        "activity_param_1": run_month_formatted,
        "stats_param_1": run_month_formatted,
        "mdnc_param_1": run_month_formatted,
        "mds_param_1": first_month_formatted,
        "mds_param_2": run_month_formatted,
        "phzcs_param_1": run_month_formatted,
        "phzc_param_1": run_month_formatted,
        "phzc_param_2": next_run_month_formatted,
    },
) as dag:
    mode = "update"

    start = DummyOperator(task_id="start")
    vhl_computation_end = vhl_computation.register(
        dag,
        start,
        mode,
    )
    activity_computation_end = activity_computation.register(
        dag,
        start,
        mode,
    )
    activity_computation_postgres_end = activity_computation_postgres.register(
        dag, activity_computation_end, mode
    )
    export_results_end = export_results.register(
        dag, activity_computation_end["Place"], mode
    )
    vhl_send_end = vhl_send.register(
        dag,
        [vhl_computation_end, activity_computation_postgres_end["Place"]],
        mode,
    )
    update_sgplaceactivity_with_zipcode_yearly_end = update_sgplaceactivity_with_zipcode_yearly.register(start=vhl_send_end, dag=dag)

# Create backfill DAG
with DAG(
    # "sg_agg_stats",
    env_args["dag_id"]+"-backfill",
    default_args=dag_args.make_default_args(
    start_date=pendulum.datetime(2022, 7, 1, tz="Europe/London"),
    retries=0,
    on_failure_callback=callbacks.task_fail_slack_alert(
        "U034YDXAD1R",  # Jake
        "U03BANPLXJR",  # Carlos
    ),
    on_retry_callback=callbacks.task_retry_slack_alert(
        "U034YDXAD1R",  # Jake
        "U03BANPLXJR",  # Carlos
    ),
    weight_rule=WeightRule.UPSTREAM,
),
    concurrency=4,
    schedule_interval=env_args["schedule_interval"],
    tags=[env_args["env"], "monthly", "backfill"],
    params=dag_args.load_config(__file__),
    # doc_md=dag_args.load_docs(__file__),
    user_defined_macros={
        "activity_param_1": prev_month_formatted,
        "stats_param_1": prev_month_formatted,
        "mdnc_param_1": first_buffer_month_formatted,
        "mds_param_1": first_month_formatted,
        "mds_param_2": prev_month_formatted,
        "phzcs_param_1": prev_month_formatted,
        "phzc_param_1": first_month_formatted,
        "phzc_param_2": run_month_formatted,
    },
) as backfill:
    mode = "backfill"

    start = DummyOperator(task_id="start", dag=backfill)
    vhl_computation_end = vhl_computation.register(
        backfill,
        start,
        mode,
    )
    activity_computation_end = activity_computation.register(
        backfill,
        start,
        mode,
    )
    vhl_send_end = vhl_send.register(
        backfill,
        [vhl_computation_end, activity_computation_end["Place"]],
        mode,
    )
