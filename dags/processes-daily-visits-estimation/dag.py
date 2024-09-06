import os
from datetime import datetime, timedelta
from importlib.machinery import SourceFileLoader

import airflow
import pendulum
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook

# local imports
from common.utils import callbacks, dag_args, slack_users

path = f"{os.path.dirname(os.path.realpath(__file__))}"

DAG_ID = path.split("/")[-1]
ENV_ARGS = dag_args.make_env_args(schedule_interval="0 6 * * *")

model_input = SourceFileLoader(
    "model_input", f"{path}/steps/model_input.py"
).load_module()
model_inference = SourceFileLoader(
    "model_inference", f"{path}/steps/model_inference.py"
).load_module()
model_output = SourceFileLoader(
    "model_output", f"{path}/steps/model_output.py"
).load_module()
final_visits = SourceFileLoader(
    "final_visits", f"{path}/steps/final_visits.py"
).load_module()
monitoring = SourceFileLoader("monitoring", f"{path}/steps/monitoring.py").load_module()
send_to_postgres = SourceFileLoader(
    "send_to_postgres", f"{path}/steps/send_to_postgres.py"
).load_module()
db_check = SourceFileLoader("db_check", f"{path}/steps/db_check.py").load_module()

invoke = SourceFileLoader("invoke", f"{path}/include/python/invoke.py").load_module()

default_args = {
    "depends_on_past": True,
}


def local_date_fn(**context) -> str:
    latency = int(Variable.get("latency_daily_feed"))
    current_date = datetime.strptime(context["ds"], "%Y-%m-%d")
    local_date = current_date - timedelta(days=latency)
    return local_date.strftime("%Y-%m-%d")


def create_dag(stage: str, manual: bool = False) -> airflow.DAG:
    dag_id = f"{DAG_ID}-{stage}-manual" if manual else f"{DAG_ID}-{stage}"
    with airflow.DAG(
        dag_id,
        schedule_interval=None if manual else ENV_ARGS["schedule_interval"],
        start_date=pendulum.datetime(2024, 5, 20, tz="Europe/London"),
        catchup=False if manual else True,
        is_paused_upon_creation=False if manual else True,
        default_args=default_args,
        max_active_runs=1,
        params={**dag_args.load_config(__file__), "stage": stage},
        on_failure_callback=callbacks.task_fail_slack_alert(slack_users.JAKE),
    ) as dag:
        start = EmptyOperator(task_id="start")

        # local_date is the airflow logical_date - latency we process SNS data.
        local_date_end = PythonOperator(
            task_id="local-date",
            python_callable=local_date_fn,
            provide_context=True,
        )
        start >> local_date_end

        # check if model alias exists, if not skip the rest of the dag.
        # if it exists, continue with the rest of the dag.
        # this ensures DAGs update dev/prod as appropriate.
        check_model_exists = PythonOperator(
            task_id="check-model-exists",
            python_callable=invoke.check_model_exists,
            op_kwargs={"name": "supervised_visits_estimation", "alias": stage},
        )
        local_date_end >> check_model_exists

        # check if the base visits table exists, if not retry until found.
        # this ensure the DAG starts from the required execution_date, whilst waiting
        # for the table to be available.
        project = Variable.get("sns_project")
        dataset = (
            dag.params["accessible-by-olvin-dataset"]
            if stage == "staging"
            else dag.params["accessible-by-olvin-almanac-dataset"]
        )
        table = f"{dag.params['postgres-rt-dataset']}-{dag.params['sgplacedailyvisitsraw-table']}"
        check_table_exists = PythonOperator(
            task_id="check-table-exists",
            python_callable=invoke.check_table_exists,
            op_kwargs={"table_id": f"{project}.{dataset}.{table}"},
            retry_delay=60 * 60 * 12,  # try ~ every 12 hrs
            retries=20,  # try for ~ 10 days
        )
        check_model_exists >> check_table_exists

        monitoring_end = monitoring.register(start=check_table_exists, dag=dag)
        model_input_end = model_input.register(start=monitoring_end, dag=dag)
        model_inference_end = model_inference.register(start=model_input_end, dag=dag)
        model_output_end = model_output.register(start=model_inference_end, dag=dag)
        final_visits_end = final_visits.register(start=model_output_end, dag=dag)
        db_check_end = db_check.register(start=final_visits_end, dag=dag)
        POSTGRES_CONN_ID = (
            dag.params["postgres-dev-conn-id"]
            if stage == "staging"
            else dag.params["postgres-staging-conn-id"]
        )
        ALMANAC_INSTANCE = (
            "almanac-dev-f9129bae"
            if stage == "staging"
            else "almanac-staging"
        )
        POSTGRES_CONN = BaseHook.get_connection(POSTGRES_CONN_ID)
        HOSTNAME = POSTGRES_CONN.host
        check_ip = PythonOperator(
            task_id="check-ip",
            python_callable=invoke.check_ip,
            op_kwargs={"almanac_ip": f"{HOSTNAME}", "almanac_instance": f"{ALMANAC_INSTANCE}"},
        )
        db_check_end >> check_ip
        send_to_pg_end = send_to_postgres.register(start=check_ip, dag=dag)

        end = EmptyOperator(task_id="end")
        send_to_pg_end >> end

    return dag


production_dag = create_dag(stage="production")
staging_dag = create_dag(stage="staging")
