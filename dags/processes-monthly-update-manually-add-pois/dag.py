import os
from importlib.machinery import SourceFileLoader

from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from common.utils import callbacks, dag_args, slack_users
from pendulum import datetime

path = f"{os.path.dirname(os.path.realpath(__file__))}"
steps_path = f"{path}/steps"
DAG_ID = path.split("/")[-1]

ingest_input_table_from_google_sheets = SourceFileLoader(
    "ingest_input_table_from_google_sheets",
    f"{steps_path}/ingest_input_table_from_google_sheets.py",
).load_module()
create_sgplaceactivity_in_manually_add_pois = SourceFileLoader(
    "create_sgplaceactivity_in_manually_add_pois",
    f"{steps_path}/create_sgplaceactivity_in_manually_add_pois.py",
).load_module()
cameo_demo_shopping = SourceFileLoader(
    "cameo_demo_shopping", f"{steps_path}/cameo_demo_shopping.py"
).load_module()
create_sgplaceraw_in_manually_add_pois = SourceFileLoader(
    "create_sgplaceraw_in_manually_add_pois",
    f"{steps_path}/create_sgplaceraw_in_manually_add_pois.py",
).load_module()
create_visits_estimation_equivalent = SourceFileLoader(
    "create_visits_estimation_equivalent",
    f"{steps_path}/create_visits_estimation_equivalent.py",
).load_module()
insert_tables_into_postgres_batch = SourceFileLoader(
    "insert_tables_into_postgres_batch",
    f"{steps_path}/insert_tables_into_postgres_batch.py",
).load_module()


env_args = dag_args.make_env_args()

default_args = dag_args.make_default_args(
    start_date=datetime(2022, 8, 16),
    retries=3,
)

with DAG(
    DAG_ID,
    default_args=default_args,
    schedule_interval=None,
    tags=[env_args["env"], "monthly", "externally_triggered"],
    params=dag_args.load_config(scope="global"),
    doc_md=dag_args.load_docs(__file__),
    on_failure_callback=callbacks.task_fail_slack_alert(
        slack_users.CARLOS, slack_users.MATIAS,
    ),
) as dag:
    start = EmptyOperator(task_id="start")

    spreadsheet_id = (
        "1sAUY2nJnn0qz67s1HH3QsdtdhVNoLJwC6kfuYVhuydo"
        if env_args["env"] == "dev"
        else "1hkZZ1jnwh6kk0QO9yQeR3LaXojH2iAnx6omrFaH-M3A"
    )

    ingest_input_table_from_google_sheets_end = (
        ingest_input_table_from_google_sheets.register(
            start=start,
            spreadsheet_id=spreadsheet_id,
            worksheet_title="input_info-pois_input_data",
        )
    )

    create_sgplaceraw_in_manually_add_pois_end = (
        create_sgplaceraw_in_manually_add_pois.register(
            start=ingest_input_table_from_google_sheets_end
        )
    )

    cameo_demo_shopping_end = cameo_demo_shopping.register(
        start=create_sgplaceraw_in_manually_add_pois_end,
    )

    create_visits_estimation_equivalent_end = (
        create_visits_estimation_equivalent.register(
            start=create_sgplaceraw_in_manually_add_pois_end, dag=dag
        )
    )

    create_sgplaceactivity_in_manually_add_pois_end = (
        create_sgplaceactivity_in_manually_add_pois.register(
            start=[
                cameo_demo_shopping_end,
                create_visits_estimation_equivalent_end,
            ]
        )
    )
    insert_tables_into_postgres_batch_end = insert_tables_into_postgres_batch.register(
        start=[
            cameo_demo_shopping_end,
            create_visits_estimation_equivalent_end,
            create_sgplaceactivity_in_manually_add_pois_end,
        ],
        dag=dag,
    )

    end = EmptyOperator(task_id="end")

    insert_tables_into_postgres_batch_end >> end
