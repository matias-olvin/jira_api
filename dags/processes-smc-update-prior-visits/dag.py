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

xcom_push = SourceFileLoader(
    "xcom_push", f"{steps_path}/xcom_push_task.py"
).load_module()

missing_brands_cats_tables = SourceFileLoader(
    "missing_brands_cats_tables", f"{steps_path}/missing_brands_categories_tables_1.py"
).load_module()

append_missing_categories = SourceFileLoader(
    "append_missing_categories", f"{steps_path}/append_missing_categories_1_5.py"
).load_module()

expert_info_contribution = SourceFileLoader(
    "expert_info_contribution", f"{steps_path}/expert_info_contribution_3.py"
).load_module()

update_brand_info_w_gtvm = SourceFileLoader(
    "update_brand_info_w_gtvm", f"{steps_path}/update_brand_info_w_gtvm_4.py"
).load_module()


env_args = dag_args.make_env_args()
default_args = dag_args.make_default_args(
    start_date=datetime(2022, 9, 16),
    retries=0,
    weight_rule=WeightRule.UPSTREAM,
    on_failure_callback=callbacks.task_fail_slack_alert(
        slack_users.MATIAS, slack_users.IGNACIO, slack_users.CARLOS
    ),
    on_retry_callback=callbacks.task_retry_slack_alert(
        slack_users.MATIAS, slack_users.IGNACIO, slack_users.CARLOS
    ),
)


with DAG(
    DAG_ID,
    default_args=default_args,
    schedule_interval=env_args["schedule_interval"],
    tags=[
        env_args["env"],
        "smc",
        "externally_triggered",
    ],
    params=dag_args.load_config(scope="global"),
    doc_md=dag_args.load_docs(__file__),
    default_view="graph",
) as dag:
    start = EmptyOperator(task_id="start")

    spreadsheet_id = (
        "15EAEgvo1VQnh5a_AI_akXIx7aBpIAC64v89RQR3RNrc"
        if env_args["env"] == "dev"
        else "1lkruQBoANmNv3WMIVXZJXJEoki32NSfT_JSO8YJNcD4"
    )
    categories_to_append_worksheet_title = "categories_to_append-update_vals"
    new_brands_worksheet_title = "new_brands-update_vals"

    xcom_push_end = xcom_push.register(start=start, dag=dag, env=env_args["env"])

    missing_brands_cats_tables_end = missing_brands_cats_tables.register(
        start=xcom_push_end
    )

    append_missing_categories_end = append_missing_categories.register(
        start=missing_brands_cats_tables_end,
        spreadsheet_id=spreadsheet_id,
        worksheet_title=categories_to_append_worksheet_title,
        env=env_args["env"],
    )

    expert_info_contribution_end = expert_info_contribution.register(
        start=append_missing_categories_end,
        spreadsheet_id=spreadsheet_id,
        worksheet_title=new_brands_worksheet_title,
        env=env_args["env"],
    )

    update_brand_info_w_gtvm_end = update_brand_info_w_gtvm.register(
        start=expert_info_contribution_end, env=env_args["env"]
    )

    end = EmptyOperator(task_id="end")

    update_brand_info_w_gtvm_end >> end
