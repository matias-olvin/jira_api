"""
DAG ID: send2postgres
"""
import os
from datetime import datetime
from importlib.machinery import SourceFileLoader

from airflow import DAG, AirflowException
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator
from common.utils import callbacks, dag_args, slack_users

path = f"{os.path.dirname(os.path.realpath(__file__))}"
steps_path = f"{path}/steps"
DAG_ID = path.split("/")[-1]

update_geom_tables = SourceFileLoader(
    "update_geom_tables", f"{steps_path}/update_geom_tables.py"
).load_module()
update_malls_tables = SourceFileLoader(
    "update_malls_tables", f"{steps_path}/update_malls_tables.py"
).load_module()
trigger_sensor_group = SourceFileLoader(
    "trigger_sensor_group", f"{steps_path}/trigger_sensor_group.py"
).load_module()

update_sgcentersraw_column = SourceFileLoader(
    "update_sgcentersraw_column", f"{steps_path}/update_sgcentersraw_column.py"
).load_module()
db_check = SourceFileLoader("db_check", f"{steps_path}/db_check.py").load_module()
send_to_database = SourceFileLoader(
    "send_to_database", f"{steps_path}/send_to_database.py"
).load_module()
send_to_gcs = SourceFileLoader(
    "send_to_gcs", f"{steps_path}/send_to_gcs.py"
).load_module()
postgres_metrics_tasks = SourceFileLoader(
    "postgres_metrics_tasks", f"{steps_path}/postgres_metrics_tasks.py"
).load_module()


trigger_postgres_batch_tasks = SourceFileLoader(
    "trigger_postgres_batch_tasks", f"{steps_path}/postgres_batch_tasks/trigger_postgres_batch.py"
).load_module()
move_ammended_tables_to_postgres_batch_tasks = SourceFileLoader(
    "move_ammended_tables_to_postgres_batch_tasks", f"{steps_path}/postgres_batch_tasks/move_ammended_tables_to_postgres_batch.py"
).load_module()
metric_collection = SourceFileLoader(
    "metric_collection", f"{steps_path}/metric_collection.py"
).load_module()
update_postgres_batch_derived_tables = SourceFileLoader(
    "update_postgres_batch_derived_tables",
    f"{steps_path}/update_postgres_batch_derived_tables.py",
).load_module()
manual_poi_adjustments = SourceFileLoader(
    "manual_poi_adjustments", f"{steps_path}/manual_poi_adjustments.py"
).load_module()
dylan_alert = SourceFileLoader(
    "dylan_alert", f"{steps_path}/dylan_alert.py"
).load_module()
deactivate_blacklist_pois = SourceFileLoader(
    "deactivate_blacklist_pois", f"{steps_path}/deactivate_blacklist_pois.py"
).load_module()
checks_after_deactivation = SourceFileLoader(
    "checks_after_deactivation", f"{steps_path}/checks_after_deactivation.py"
).load_module()
sgplacebenchmark = SourceFileLoader(
    "sgplacebenchmark", f"{steps_path}/sgplacebenchmark.py"
).load_module()
sgcenterbenchmark = SourceFileLoader(
    "sgcenterbenchmark", f"{steps_path}/sgcenterbenchmark.py"
).load_module()
sgbrandbenchmark = SourceFileLoader(
    "sgbrandbenchmark", f"{steps_path}/sgbrandbenchmark.py"
).load_module()
postgres_version = SourceFileLoader(
    "postgres_version", f"{steps_path}/postgres_version.py"
).load_module()
send_to_marketing = SourceFileLoader(
    "send_to_marketing", f"{steps_path}/marketing_tables.py"
).load_module()
materialize_views = SourceFileLoader(
    "materialize_views", f"{steps_path}/materialize_views.py"
).load_module()
daily_visits_copies = SourceFileLoader(
    "daily_visits_copies", f"{steps_path}/daily_visits_copies.py"
).load_module()

env_args = dag_args.make_env_args()
default_args = dag_args.make_default_args(
    start_date=datetime(2022, 8, 16),
    retries=3,
    email="kartikey@olvin.com",
    email_on_failure=True,
    email_on_retry=False,
)

with DAG(
    DAG_ID,
    default_args=default_args,
    schedule_interval=None,
    tags=[env_args["env"], "monthly"],
    params=dag_args.load_config(__file__),
    doc_md=dag_args.load_docs(__file__),
    on_failure_callback=callbacks.task_fail_slack_alert(
        slack_users.MATIAS, slack_users.IGNACIO
    ),
) as dag:
    start = DummyOperator(task_id="start")

    update_geom_tables_end = update_geom_tables.register(dag, start, postgres_dataset='postgres')

    update_malls_tables_end = update_malls_tables.register(dag, start, postgres_dataset='postgres')


    trigger_sensor_group_postgres_tasks = trigger_sensor_group.register(
        start=update_malls_tables_end,
        dag=dag,
        external_dag_id="products-visits-to-malls",
        use_postgres_batch_dataset=False
    )

    update_sgcentersraw_column_postgres_task = update_sgcentersraw_column.register(start=trigger_sensor_group_postgres_tasks, dag=dag, postgres_dataset='postgres')

    preprocess_tables_end = DummyOperator(task_id="preprocess_tables_end")
    

    [update_geom_tables_end, update_sgcentersraw_column_postgres_task] >> preprocess_tables_end

    sgplacebenchmark_postgres_end = sgplacebenchmark.register(dag, preprocess_tables_end, postgres_dataset='postgres')

    sgbrandbenchmark_postgres_end = sgbrandbenchmark.register(start=sgplacebenchmark_postgres_end, dag=dag, postgres_dataset="postgres")

    sgcenterbenchmark_postgres_end = sgcenterbenchmark.register(start=sgplacebenchmark_postgres_end, dag=dag, postgres_dataset="postgres")

    sgcenter_sgbrand_raw_benchmarking_end = EmptyOperator(
        task_id="sgcenter_sgbrand_raw_benchmarking_end",
    )

    [sgbrandbenchmark_postgres_end, sgcenterbenchmark_postgres_end] >> sgcenter_sgbrand_raw_benchmarking_end

    postgres_metrics_tasks_end = postgres_metrics_tasks.register(
        dag, sgcenter_sgbrand_raw_benchmarking_end
    )

    raw_metric_collection_end = metric_collection.register(
        dag, postgres_metrics_tasks_end, "raw"
    )

    def task_to_fail():
        """
        Task that will fail.
        """
        raise AirflowException("This task must be manually set to success to continue.")

    check_postgres_metrics_results = PythonOperator(
        task_id="check_postgres_metrics_results",
        python_callable=task_to_fail,
        dag=dag,
    )
    raw_metric_collection_end >> check_postgres_metrics_results

    send_to_gcs_end = send_to_gcs.register(
        dag, check_postgres_metrics_results, final=False
    )

    trigger_postgres_batch_tasks_end = trigger_postgres_batch_tasks.register(start=send_to_gcs_end, dag=dag)

    move_ammended_tables_to_postgres_batch_tasks_end = move_ammended_tables_to_postgres_batch_tasks.register(
        start=trigger_postgres_batch_tasks_end, dag=dag
    )

    final_metric_collection_end = metric_collection.register(
        dag, move_ammended_tables_to_postgres_batch_tasks_end, "final"
    )

    check_postgres_batch_results = PythonOperator(
        task_id="check_postgres_batch_results",
        python_callable=task_to_fail,
        dag=dag,
    )
    final_metric_collection_end >> check_postgres_batch_results


    # Making inactive the blacklisted POIs
    deactivate_blacklist_pois_end = deactivate_blacklist_pois.register(dag, check_postgres_batch_results)

    # # Same as above, but for final tables
    update_geom_tables_final_end = update_geom_tables.register(dag, deactivate_blacklist_pois_end,
                                                               postgres_dataset='postgres_batch')

    update_malls_tables_final_end = update_malls_tables.register(dag, deactivate_blacklist_pois_end,
                                                                 postgres_dataset='postgres_batch')
    

    trigger_sensor_group_postgres_batch_tasks = trigger_sensor_group.register(
        start=update_malls_tables_final_end,
        dag=dag,
        external_dag_id="products-visits-to-malls",
        use_postgres_batch_dataset=True
    )

    update_sgcentersraw_column_postgres_batch_task = update_sgcentersraw_column.register(start=trigger_sensor_group_postgres_batch_tasks, dag=dag, postgres_dataset='postgres_batch')

    preprocessed_tables_after_deactivations_end = DummyOperator(
        task_id="preprocessed_tables_after_deactivations_end"
    )

    [update_geom_tables_final_end, update_sgcentersraw_column_postgres_batch_task] >> preprocessed_tables_after_deactivations_end

    checks_after_deactivation_end = checks_after_deactivation.register(dag, preprocessed_tables_after_deactivations_end)

    sgplacebenchmark_postgres_batch_end = sgplacebenchmark.register(dag, checks_after_deactivation_end,
                                                    postgres_dataset='postgres_batch')

    sgbrandbenchmark_postgres_batch_end = sgbrandbenchmark.register(start=sgplacebenchmark_postgres_batch_end, dag=dag, postgres_dataset="postgres_batch")

    sgcenterbenchmark_postgres_batch_end = sgcenterbenchmark.register(start=sgplacebenchmark_postgres_batch_end, dag=dag, postgres_dataset="postgres_batch")

    sgcenter_sgbrand_final_benchmarking_end = EmptyOperator(
        task_id="sgcenter_sgbrand_final_benchmarking_end",
    )

    [sgbrandbenchmark_postgres_batch_end, sgcenterbenchmark_postgres_batch_end] >> sgcenter_sgbrand_final_benchmarking_end
    
    postgres_derived_end = update_postgres_batch_derived_tables.register(dag, sgcenter_sgbrand_final_benchmarking_end)

    manual_poi_adjustments_end = manual_poi_adjustments.register(start=postgres_derived_end, dag=dag)

    dylan_alert_end = dylan_alert.register(dag, manual_poi_adjustments_end)

    db_check_end = db_check.register(start=manual_poi_adjustments_end, dag=dag)
    
    postgres_version_end = postgres_version.register(start=db_check_end, dag=dag)

    materialize_views_end = materialize_views.register(start=postgres_version_end, dag=dag)

    daily_visits_copies_end = daily_visits_copies.register(start=materialize_views_end, dag=dag)

    start_sending_to_backend = PythonOperator(
        task_id="start_sending_to_backend",
        python_callable=task_to_fail,
        dag=dag,
    )

    daily_visits_copies_end >> start_sending_to_backend

    send_to_database_end = send_to_database.register(dag, start_sending_to_backend, db="dev")

    send_to_gcs_final_end = send_to_gcs.register(dag, send_to_database_end, final=True)

    send_to_marketing_end = send_to_marketing.register(dag, send_to_gcs_final_end)
