from __future__ import annotations

import os
from importlib.machinery import SourceFileLoader

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from common.utils import callbacks, dag_args

path = f"{os.path.dirname(os.path.realpath(__file__))}"
DAG_ID = path.split("/")[-1]

# PLACES
ingest_input_table_from_google_sheets = SourceFileLoader(
    "ingest_input_table_from_google_sheets",
    f"{path}/steps/places/ingest_input_table_from_google_sheets.py",
).load_module()
places_tasks = SourceFileLoader(
    "places-tasks", f"{path}/steps/places/places-tasks.py"
).load_module()
places_tests = SourceFileLoader(
    "places-tests", f"{path}/steps/places/places-tests.py"
).load_module()
# NAICS CODES CHECK
update_new_naics_codes_sensitivity_check = SourceFileLoader(
    "update_new_naics_codes_sensitivity_check",
    f"{path}/steps/naics_codes/update_new_naics_codes_sensitivity_check.py",
).load_module()
update_sg_categories_match = SourceFileLoader(
    "update_sg_categories_match",
    f"{path}/steps/naics_codes/update_sg_categories_match.py",
).load_module()
update_naics_code_subcategories = SourceFileLoader(
    "update_naics_code_subcategories",
    f"{path}/steps/naics_codes/update_naics_code_subcategories.py",
).load_module()
update_different_digit_naics_code = SourceFileLoader(
    "update_different_digit_naics_code",
    f"{path}/steps/naics_codes/update_different_digit_naics_code.py",
).load_module()
insert_naics_tables = SourceFileLoader(
    "insert_naics_tables", f"{path}/steps/naics_codes/insert_naics_tables.py"
).load_module()
# BRANDS
brands_tasks = SourceFileLoader(
    "brands-tasks", f"{path}/steps/brands/brands-tasks.py"
).load_module()
brands_tests = SourceFileLoader(
    "brands-tests", f"{path}/steps/brands/brands-tests.py"
).load_module()
# SPEND
spend_tasks = SourceFileLoader(
    "spend-tasks", f"{path}/steps/spend/spend-tasks.py"
).load_module()

env_args = dag_args.make_env_args()

default_args = dag_args.make_default_args(
    start_date=pendulum.datetime(2022, 3, 1, tz="Europe/London"),
    retries=2,
    on_retry_callback=callbacks.task_retry_slack_alert(
        "U034YDXAD1R", channel=env_args["env"]
    ),
    on_failure_callback=callbacks.task_fail_slack_alert(
        "U034YDXAD1R", channel=env_args["env"]
    ),
)

with DAG(
    DAG_ID,
    default_args=default_args,
    description="Update `sg_places` tables for SMC.",
    schedule_interval=env_args["schedule_interval"],
    tags=[env_args["env"], "smc", "externally_triggered"],
    params=dag_args.load_config(scope="global"),
    doc_md=dag_args.load_docs(__file__),
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    brands_tasks_end = brands_tasks.register(start=start)
    brands_tests_end = brands_tests.register(start=brands_tasks_end)

    spreadsheet_id = (
        "1sAUY2nJnn0qz67s1HH3QsdtdhVNoLJwC6kfuYVhuydo"
        if env_args["env"] == "dev"
        else "1hkZZ1jnwh6kk0QO9yQeR3LaXojH2iAnx6omrFaH-M3A"
    )

    ingest_input_table_from_google_sheets_end = (
        ingest_input_table_from_google_sheets.register(
            start=brands_tests_end,
            spreadsheet_id=spreadsheet_id,
            worksheet_title="input_info-pois_input_data",
        )
    )

    places_tasks_end = places_tasks.register(
        start=ingest_input_table_from_google_sheets_end
    )
    places_tests_end = places_tests.register(start=places_tasks_end)

    update_new_naics_codes_sensitivity_check_end = (
        update_new_naics_codes_sensitivity_check.register(start=places_tests_end)
    )

    update_different_digit_naics_code_end = update_different_digit_naics_code.register(
        start=update_new_naics_codes_sensitivity_check_end
    )
    update_sg_categories_match_end = update_sg_categories_match.register(
        start=update_new_naics_codes_sensitivity_check_end
    )
    update_naics_code_subcategories_end = update_naics_code_subcategories.register(
        start=update_new_naics_codes_sensitivity_check_end
    )

    update_dif_cat_sub_tasks = [
        update_different_digit_naics_code_end,
        update_sg_categories_match_end,
        update_naics_code_subcategories_end,
    ]

    insert_naics_tables_end = insert_naics_tables.register(
        start=update_dif_cat_sub_tasks
    )

    spend_tasks_end = spend_tasks.register(start=start)

    [insert_naics_tables_end, spend_tasks_end] >> end
