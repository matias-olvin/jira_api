"""
DAG ID: site_analysis
"""
import os,pendulum
from datetime import datetime
from importlib.machinery import SourceFileLoader
from common.utils import dag_args, callbacks

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
)
path = f"{os.path.dirname(os.path.realpath(__file__))}"
steps_path = path + "/steps"
site_geometry = SourceFileLoader(
    "site_geometry", f"{steps_path}/site_geometry.py"
).load_module()
site_place_id = SourceFileLoader(
    "site_place_id", f"{steps_path}/site_place_id.py"
).load_module()
visitors_homes = SourceFileLoader(
    "visitors_homes", f"{steps_path}/visitors_homes.py"
).load_module()
visitors_demographics = SourceFileLoader(
    "visitors_demographics", f"{steps_path}/visitors_demographics.py"
).load_module()
trade_area = SourceFileLoader("trade_area", f"{steps_path}/trade_area.py").load_module()
site_geometry_one_mile = SourceFileLoader(
    "site_geometry_one_mile", f"{steps_path}/site_geometry_one_mile.py"
).load_module()
psychographics_and_void = SourceFileLoader(
    "psychographics_and_void", f"{steps_path}/psychographics_and_void.py"
).load_module()
export_results = SourceFileLoader(
    "export_results", f"{steps_path}/export_results.py"
).load_module()
visits_analysis = SourceFileLoader(
    "visits_analysis", f"{steps_path}/visits_analysis.py"
).load_module()
chain_ranking = SourceFileLoader(
    "chain_ranking", f"{steps_path}/chain_ranking.py"
).load_module()

DAG_ID = path.split("/")[-1]

env_args = dag_args.make_env_args(
    dag_id=DAG_ID,
)

dag = DAG(
    env_args["dag_id"],
    default_args=dag_args.make_default_args(
    start_date=pendulum.datetime(2021, 6, 1, tz="Europe/London"),
    end_date=None,
    on_failure_callback=callbacks.task_fail_slack_alert(
        "U0180QS8MFV", "U030QACBWDU"
    ),  # , "U0297RS89KQ"),
    on_retry_callback=callbacks.task_retry_slack_alert(
        "U0180QS8MFV", "U030QACBWDU"
    ),  # , "U0297RS89KQ"),
    retries=1,
),
    schedule_interval=env_args["schedule_interval"],
    tags=[env_args["env"], "misc"],
    params=dag_args.load_config(__file__),
    doc_md=dag_args.load_docs(__file__),
    default_view="graph",
)
start = DummyOperator(task_id="start", dag=dag)

create_dataset = BigQueryCreateEmptyDatasetOperator(
    task_id="create_dataset",
    project_id=Variable.get("storage_project_id"),
    dataset_id="{{ dag_run.conf['site_name'] }}",
    dag=dag,
    location="EU",
)
start >> create_dataset


def site_selection_branch(**kwargs):
    if (
        len(kwargs["dag_run"].conf["select_geometry"]) == 0
        and len(kwargs["dag_run"].conf["remove_geometry"]) == 0
        and len(kwargs["dag_run"].conf["place_ids"]) > 0
    ):
        return "place_ids_start"
    elif (
        len(kwargs["dag_run"].conf["select_geometry"]) > 0
        and len(kwargs["dag_run"].conf["remove_geometry"]) > 0
        and len(kwargs["dag_run"].conf["place_ids"]) == 0
    ):
        return "geometry_start"
    else:
        raise ValueError(
            "One and only one of the parameters 'select_geometry' and 'remove_geometry' or 'place_ids' "
            "must not be empty."
        )


site_selection_branching = BranchPythonOperator(
    task_id="site_selection_branching",
    python_callable=site_selection_branch,
    provide_context=True,
    dag=dag,
)
place_ids_start = DummyOperator(task_id="place_ids_start", dag=dag)
geometry_start = DummyOperator(task_id="geometry_start", dag=dag)
geometry_one_mile_buffer_start = DummyOperator(
    task_id="geometry_one_mile_buffer_start", dag=dag
)
visits_analysis_start = DummyOperator(task_id="visits_analysis_start", dag=dag)
chain_ranking_start = DummyOperator(task_id="chain_ranking_start", dag=dag)

create_dataset >> site_selection_branching >> [place_ids_start, geometry_start]
create_dataset >> [
    geometry_one_mile_buffer_start,
    visits_analysis_start,
    chain_ranking_start,
]

geometry_one_mile_buffer_end = site_geometry_one_mile.register(
    dag, geometry_one_mile_buffer_start
)
psychographics_and_void_end = psychographics_and_void.register(
    dag, geometry_one_mile_buffer_end
)

visits_analysis_end = visits_analysis.register(dag, visits_analysis_start)
chain_ranking_end = chain_ranking.register(dag, chain_ranking_start)

site_geometry_end = site_geometry.register(dag, geometry_start)
site_place_id_end = site_place_id.register(dag, place_ids_start)

site_selection_end = DummyOperator(
    task_id="site_selection_end", dag=dag, trigger_rule="none_failed"
)
[
    site_geometry_end,
    site_place_id_end,
    visits_analysis_end,
    chain_ranking_end,
    psychographics_and_void_end,
] >> site_selection_end

visitors_homes_end = visitors_homes.register(dag, site_selection_end)

visitors_demographics_end = visitors_demographics.register(dag, visitors_homes_end)
trade_area_end = trade_area.register(dag, visitors_homes_end)

export_results_end = export_results.register(
    dag, [visitors_demographics_end, trade_area_end]
)

end = DummyOperator(
    task_id="end",
    dag=dag,
    on_success_callback=callbacks.pipeline_end_slack_alert("U0180QS8MFV"),
)
export_results_end >> end
