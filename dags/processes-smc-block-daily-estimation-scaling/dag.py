import os
from importlib.machinery import SourceFileLoader

import pendulum
from airflow.models import DAG, Variable
from airflow.operators.empty import EmptyOperator
from airflow.utils.weight_rule import WeightRule
from common.utils import callbacks, dag_args, slack_users

path = os.path.dirname(os.path.realpath(__file__))
DAG_ID = path.split("/")[-1]
steps_path = f"{path}/steps"

scaling_block_daily_estimation_bq_api = SourceFileLoader(
    "scaling_block_daily_estimation_bq_api", f"{steps_path}/scaling_block_daily_estimation_bq_api.py"
).load_module()

env_args = dag_args.make_env_args()
default_args = dag_args.make_default_args(
    start_date=pendulum.datetime(2022, 9, 16, tz="Europe/London"),
    retries=3,
    weight_rule=WeightRule.UPSTREAM,
    on_failure_callback=callbacks.task_fail_slack_alert(slack_users.MATIAS),
    on_retry_callback=callbacks.task_retry_slack_alert(slack_users.MATIAS),
)


with DAG(
    DAG_ID,
    default_args=default_args,
    schedule_interval=env_args["schedule_interval"],
    tags=[env_args["env"], "smc", "externally triggered"],
    max_active_tasks=35,
    params=dag_args.load_config(scope="global"),
    doc_md=dag_args.load_docs(__file__),
    default_view="graph",
) as dag:
    start = EmptyOperator(task_id="start")

    smc_start_date = Variable.get("smc_start_date")

    scaling_block_daily_estimation_bq_api_end = scaling_block_daily_estimation_bq_api.register(
        start=start,
        dag=dag,
        env="dev",
        start_date="2018-01-01",
        end_date=smc_start_date,
    )

    end = EmptyOperator(
        task_id="end",
        on_success_callback=callbacks.task_end_custom_slack_alert(
            "U03BANPLXJR",
            msg_title="*POI Visits Scaled Block daily estimation* created using the new models.",
        ),
    )

    scaling_block_daily_estimation_bq_api_end >> end
