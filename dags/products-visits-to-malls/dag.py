import os
from importlib.machinery import SourceFileLoader

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from common.utils import callbacks, dag_args

path = f"{os.path.dirname(os.path.realpath(__file__))}"
DAG_ID = path.split("/")[-1]
steps_path = f"{path}/steps"

xcom_setter = SourceFileLoader(
    "xcom_tasks", f"{steps_path}/x_com_task/x_com_push_task.py"
).load_module()

malls_gt_agg = SourceFileLoader(
    "malls_gt_agg_tasks", f"{steps_path}/malls_aggregation_table/malls_gt_agg_tasks.py"
).load_module()

ingestion_of_training_data = SourceFileLoader(
    "ingestion_of_training_data",
    f"{steps_path}/ingestion_of_training_data/ingestion_of_training_data.py",
).load_module()

create_and_run_model = SourceFileLoader(
    "create_and_run_model_tasks",
    f"{steps_path}/model_creation_migration/model_creation_migration_tasks.py",
).load_module()

scaling_to_malls = SourceFileLoader(
    "scaling_to_malls_tasks",
    f"{steps_path}/scaling_to_all_US_malls/scaling_to_malls_tasks.py",
).load_module()

validations = SourceFileLoader(
    "validation_tasks", f"{steps_path}/validations/validation_tasks.py"
).load_module()

verifications = SourceFileLoader(
    "verification_tasks", f"{steps_path}/verifications/verification_tasks.py"
).load_module()

env_args = dag_args.make_env_args()
default_args = dag_args.make_default_args(
    start_date=pendulum.datetime(2022, 8, 16, tz="Europe/London"),
    retries=0,
    provide_context=True,
)

with DAG(
    DAG_ID,
    params=dag_args.load_config(scope="global"),
    default_args=default_args,
    schedule_interval=env_args["schedule_interval"],
    tags=[env_args["env"], "postgres", "externally_triggered"],
    doc_md=dag_args.load_docs(__file__),
    on_failure_callback=callbacks.task_fail_slack_alert(
        "U05FN3F961X", "U05M60N8DMX", channel=env_args["env"]  # Ignacio, Matias
    ),
) as dag:

    spreadsheet_id = (
        "13J4n2Ah93CJUny7q5Geja231ZyJ8fqvcM85gHFBwsDM"
        if env_args["env"] == "dev"
        else "1rbOFQxWiJdvBelTztX83BrbcNpr5vqiQeDn-EkMKJp8"
    )
    worksheet_title = "training_data-dataset_used_for_random_forest_reg"

    start = EmptyOperator(task_id="start")

    xcom_tasks = xcom_setter.register(start=start, env=env_args["env"])

    malls_gt_agg_tasks = malls_gt_agg.register(start=xcom_tasks)

    ingestion_of_training_data_tasks = ingestion_of_training_data.register(
        start=malls_gt_agg_tasks,
        spreadsheet_id=spreadsheet_id,
        worksheet_title=worksheet_title,
    )

    create_and_run_model_tasks = create_and_run_model.register(
        start=ingestion_of_training_data_tasks, env=env_args["env"]
    )

    scaling_to_malls_tasks = scaling_to_malls.register(start=create_and_run_model_tasks)

    validation_tasks = validations.register(start=scaling_to_malls_tasks)

    verification_tasks = verifications.register(start=validation_tasks)

    end = EmptyOperator(task_id="end")

    verification_tasks >> end
