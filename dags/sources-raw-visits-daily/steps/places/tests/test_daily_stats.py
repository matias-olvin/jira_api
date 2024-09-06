"""
DAG ID: poi_visits_pipeline_all_daily
"""
from datetime import datetime, timedelta

from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator

# from airflow.exceptions import AirflowFailException
from airflow.operators.latest_only_operator import LatestOnlyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryValueCheckOperator,
)
from common.utils import callbacks, slack_users


def register(dag, start):
    """
    Register tasks on the dag

    Args:
        dag (airflow.models.DAG): DAG instance to register tasks on.
        start (airflow.models.TaskInstance): Task instance all tasks will
        be registered downstream from.
        mode (str): "update" or "backfill"

    Returns:
        airflow.models.TaskInstance: The last task node in this section.
    """

    # UPDATE TASKS
    test_tasks_end = DummyOperator(
        task_id="tests_end", dag=dag, depends_on_past=False, trigger_rule="all_done"
    )

    skip_if_backfill = LatestOnlyOperator(
        task_id="skip_tests_if_backfill",
        wait_for_downstream=False,
        depends_on_past=False,
    )
    start >> skip_if_backfill

    # Test that Tamoco raw data is in GCS bucket.
    def blob_path(date, **kwargs):
        """
        Create blob path from date.
        Returns: blob path
        """
        year = date.split("-")[0]
        month = date.split("-")[1]
        day = date.split("-")[2]
        blob = f"{year}/{month}/{day}/"

        return blob

    def run_test(
        task_id: str, test_name: str, sql_file_name: str, start=skip_if_backfill
    ):
        delete_task = BigQueryInsertJobOperator(
            task_id=f"delete_{task_id}",
            project_id=dag.params["project"],
            configuration={
                "query": {
                    "query": f"""
                        DELETE FROM
                            `{{{{ params['project'] }}}}.{{{{ params['test_compilation_dataset'] }}}}.{{{{ params['test_results_table'] }}}}`
                        WHERE
                            pipeline = 'visits_pipeline_all_daily' AND
                            test_name = '{test_name}' AND
                            run_date = '{{{{ ds }}}}'
                    """,
                    "useLegacySql": "False",
                },
                "labels": {
                    "pipeline": "{{ dag.dag_id }}",
                    "task_id": "{{ task.task_id.lower()[:63] }}",
                },
            },
            dag=dag,
            location="EU",
            wait_for_downstream=False,
            depends_on_past=False,
            trigger_rule="one_success",
        )
        test_task = BigQueryInsertJobOperator(
            task_id=f"run_{task_id}",
            project_id=dag.params["project"],
            configuration={
                "query": {
                    "query": f"{{% include './bigquery/places/tests/{sql_file_name}.sql' %}}",
                    "useLegacySql": "False",
                },
                "labels": {
                    "pipeline": "{{ dag.dag_id }}",
                    "task_id": "{{ task.task_id.lower()[:63] }}",
                },
            },
            dag=dag,
            location="EU",
            depends_on_past=False,
            wait_for_downstream=False,
            trigger_rule="one_success",
        )
        check_task = BigQueryValueCheckOperator(
gcp_conn_id="google_cloud_olvin_default",
            task_id=f"check_{task_id}",
            sql=f"""
                SELECT 
                    result 
                FROM 
                    `{{{{ params['project'] }}}}.{{{{ params['test_compilation_dataset'] }}}}.{{{{ params['test_results_table'] }}}}` 
                WHERE 
                    pipeline = 'visits_pipeline_all_daily' AND
                    test_name = '{test_name}' AND
                    run_date = '{{{{ ds }}}}'
            """,
            pass_value=True,
            dag=dag,
            location="EU",
            use_legacy_sql=False,
            depends_on_past=False,
            wait_for_downstream=False,
            retries=0,
            on_failure_callback=callbacks.task_fail_slack_alert(
                slack_users.MATIAS, channel="prod"
            ),
            trigger_rule="one_success",
        )
        start >> delete_task >> test_task >> check_task

        return check_task

    test_raw_tamoco_size_diff = run_test(
        task_id="test_raw_tamoco_size_diff",
        test_name="tamoco raw data size difference",
        sql_file_name="check_raw_tamoco_size_diff",
    )
    test_raw_tamoco_size_diff >> test_tasks_end

    test_day_stats_visits_scaled = run_test(
        task_id="test_day_stats_visits_scaled",
        test_name="day stats visits scaled",
        sql_file_name="day_stats_visits_scaled_test",
    )
    write_day_stats_visits_scaled = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="write_day_stats_visits_scaled",
        project_id=dag.params["project"],
        configuration={
            "query": {
                "query": "{% include './bigquery/places/tests/write_day_stats_visits_scaled_test.sql' %}",
                "useLegacySql": "False",
                "destinationTable": {
                    "projectId": "{{ params['project'] }}",
                    "datasetId": "{{ params['test_failures_dataset'] }}",
                    "tableId": "day_stats_visits_scaled_{{ ds_nodash }}",
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_IF_NEEDED",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        wait_for_downstream=False,
        depends_on_past=False,
        trigger_rule="one_failed",
        dag=dag,
        location="EU",
    )
    test_day_stats_visits_scaled >> write_day_stats_visits_scaled >> test_tasks_end

    test_latency = run_test(
        task_id="test_latency", test_name="10 day latency", sql_file_name="latency_test"
    )
    test_latency >> test_tasks_end

    return test_tasks_end
