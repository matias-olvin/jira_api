"""
DAG ID: visits_estimation
"""
import os, pendulum
from datetime import datetime
from importlib.machinery import SourceFileLoader
from common.utils import dag_args, callbacks

from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.dummy_operator import DummyOperator
from common.utils import slack_users

path = f"{os.path.dirname(os.path.realpath(__file__))}"
steps_path = path + "/steps"

DAG_ID = path.split("/")[-1]

env_args = dag_args.make_env_args(
    dag_id=DAG_ID,
)

geospatial_path = f"{steps_path}/geospatial/"
geospatial = SourceFileLoader(
    "geospatial", geospatial_path + "geospatial.py"
).load_module()

trigger_sensor_group = SourceFileLoader(
    "trigger_sensor_group", f"{steps_path}/trigger_sensor_group/trigger_sensor_group.py"
).load_module()

general_path = f"{steps_path}/general/"
general = SourceFileLoader("general", general_path + "general.py").load_module()

ground_truth_path = f"{steps_path}/ground_truth/"
supervised = SourceFileLoader(
    "supervised", ground_truth_path + "supervised.py"
).load_module()

quality_path = f"{steps_path}/quality/"
quality = SourceFileLoader("quality", quality_path + "quality.py").load_module()

adjustments_path = f"{steps_path}/adjustments/"
events = SourceFileLoader("events", adjustments_path + "events.py").load_module()
covid = SourceFileLoader("covid", adjustments_path + "covid.py").load_module()
closings = SourceFileLoader("closings", adjustments_path + "closings.py").load_module()
volume = SourceFileLoader("volume", adjustments_path + "volume.py").load_module()
places_dynamic = SourceFileLoader(
    "places_dynamic", adjustments_path + "places_dynamic.py"
).load_module()
hourly = SourceFileLoader("hourly", adjustments_path + "hourly.py").load_module()

postgres_path = os.path.dirname(os.path.realpath(__file__)) + "/steps/postgres/"
data_preparation = SourceFileLoader(
    "data_preparation", postgres_path + "data_preparation.py"
).load_module()
sending2postgres = SourceFileLoader(
    "sending2postgres", postgres_path + "sending2postgres.py"
).load_module()

my_var = True

with DAG(
    env_args["dag_id"],
    default_args=dag_args.make_default_args(
    start_date=pendulum.datetime(2022, 1, 8, tz="Europe/London"),
    retries=0,
    on_failure_callback=callbacks.task_fail_slack_alert(
        slack_users.MATIAS, slack_users.IGNACIO, channel=env_args["env"]
    ),
),
    concurrency=12,
    schedule_interval=env_args["schedule_interval"],
    tags=[env_args["env"], "monthly"],
    params=dag_args.load_config(__file__),
    doc_md=dag_args.load_docs(__file__),
) as dag:
    start = DummyOperator(task_id="start", dag=dag)

    # geospatial
    geospatial_end = geospatial.register(dag, start, env_args["env"])

    # dataforseo
    dataforseo_end = trigger_sensor_group.register(start=start, dag=dag, trigger_dag_id="dag-sources-regressors-dataforseo-trend")

    # general
    general_end = general.register(dag, geospatial_end)

    # ground_truth
    supervised_end = supervised.register(dag, [general_end, dataforseo_end], env_args["env"])

    # quality
    quality_end = quality.register(dag, supervised_end, env_args["env"])

    # adjustments
    events_end = events.register(dag, quality_end, env_args["env"])
    closings_end = closings.register(dag, events_end, env_args["env"])
    covid_end = covid.register(dag, closings_end, env_args["env"])
    volume_end = volume.register(dag, covid_end, env_args["env"])
    places_dynamic_end = places_dynamic.register(dag, volume_end, env_args["env"])
    hourly_end = hourly.register(dag, places_dynamic_end, env_args["env"])

    # Postgres
    data_preparation_end = data_preparation.register(dag, hourly_end)

    trigger_postgres = TriggerDagRunOperator(
        task_id="trigger_postgres",
        trigger_dag_id=env_args["dag_id"]+"-postgres",
        execution_date="{{ ds }}",
    )

    data_preparation_end >> trigger_postgres


with DAG(
    env_args["dag_id"]+"-postgres",
    default_args=dag_args.make_default_args(
    start_date=pendulum.datetime(2022, 1, 8, tz="Europe/London"),
    retries=0,
    on_failure_callback=callbacks.task_fail_slack_alert(
        slack_users.MATIAS, slack_users.IGNACIO, channel=env_args["env"]
    ),
    ),
    concurrency=12,
    schedule_interval=env_args["schedule_interval"],
    tags=[env_args["env"], "monthly"],
    params=dag_args.load_config(__file__),
    doc_md=dag_args.load_docs(__file__),
) as postgres_dag:
    start = DummyOperator(task_id="start", dag=postgres_dag)
    sending2postgres_end = sending2postgres.register(postgres_dag, start)
