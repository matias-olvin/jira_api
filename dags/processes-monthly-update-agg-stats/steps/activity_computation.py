"""
DAG ID: sg_agg_stats
"""
import os
from importlib.machinery import SourceFileLoader

from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryValueCheckOperator,
)
from common.utils import callbacks

steps_path = f"{os.path.dirname(os.path.realpath(__file__))}/demographics_tests"
export_results = SourceFileLoader(
    "export_results", f"{steps_path}/export_results.py"
).load_module()


def register(dag, start, mode):
    """Register tasks on the dag.

    Args:
        dag (airflow.models.DAG): DAG instance to register tasks on.
        start (airflow.models.TaskInstance): Task instance all tasks will
            registered downstream from.

    Returns:
        airflow.models.TaskInstance: The last task node in this section.
    """
    final_tasks = {}
    env_project = Variable.get("env_project")

    def results_activity(
        dataset: str, table: str, insert: str, location: str, location_key: str
    ):

        # Compute results
        if mode == "update":
            activity_table = (
                f"{env_project}.{dag.params[f'{dataset}_dataset']}."
                f"{dag.params[f'{table}_activity_table']}"
            )

            delete_job = BigQueryInsertJobOperator(
                task_id=f"delete_run_{table}_{dataset}",
                project_id=env_project,
                configuration={
                    "query": {
                        "query": f"DELETE FROM `{activity_table}` "
                        f"WHERE run_date = '{{{{ ds }}}}'",
                        "useLegacySql": "False",
                    },
                    "labels": {"sg_stats": "pipeline"},
                },
                dag=dag,
                location="EU",
            )
        elif mode == "backfill":
            activity_table = (
                f"{env_project}.{dag.params[f'{dataset}_dataset']}."
                f"{dag.params[f'{table}_activity_table']}"
            )
            delete_job = BigQueryInsertJobOperator(
                task_id=f"delete_run_{table}_{dataset}",
                project_id=env_project,
                configuration={
                    "query": {
                        "query": f"DELETE FROM `{activity_table}` "
                        f"WHERE run_date = '{{{{ ds }}}}'",
                        "useLegacySql": "False",
                    },
                    "labels": {
                        "pipeline": "{{ dag.dag_id }}",
                        "task_id": "{{ task.task_id.lower()[:63] }}",
                    },
                },
                dag=dag,
                location="EU",
            )

        if location == "poi":
            query_job = BigQueryInsertJobOperator(
                task_id=f"query_{table}_{dataset}",
                configuration={
                    "query": {
                        "query": "{% include './bigquery/activity_places.sql' %}",
                        "useLegacySql": "False",
                    },
                    "labels": {
                        "pipeline": "{{ dag.dag_id }}",
                        "task_id": "{{ task.task_id.lower()[:63] }}",
                    },
                },
                dag=dag,
                location="EU",
                depends_on_past=True,
                wait_for_downstream=True,
            )

        elif location == "zipcode":

            query_job = BigQueryInsertJobOperator(
                task_id=f"query_{table}_{dataset}",
                project_id=env_project,
                configuration={
                    "query": {
                        "query": "{% include './bigquery/activity_zipcodes.sql' %}",
                        "useLegacySql": "False",
                    },
                    "labels": {
                        "pipeline": "{{ dag.dag_id }}",
                        "task_id": "{{ task.task_id.lower()[:63] }}",
                    },
                },
                dag=dag,
                location="EU",
                depends_on_past=True,
                wait_for_downstream=True,
            )

        delete_job >> query_job

        # Create result check table
        check_table = BigQueryInsertJobOperator(
            task_id=f"query_{table}_{dataset}_check",
            project_id=env_project,
            configuration={
                "query": {
                    "query": f"{{% with "
                    f"final_table='{env_project}."
                    f"{dag.params[f'{dataset}_dataset']}."
                    f"{dag.params[f'{table}_activity_table']}_check', "
                    f"original_table='{env_project}."
                    f"{dag.params[f'{dataset}_dataset']}."
                    f"{dag.params[f'{table}_activity_table']}'"
                    f"%}}{{% include './bigquery/{dataset}_NumberCheck.sql' %}}{{% endwith %}}",
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
        )
        query_job >> check_table

        check_change_task = BigQueryValueCheckOperator(
            gcp_conn_id="google_cloud_olvin_default",
            sql=f"SELECT COUNT(*) FROM `{env_project}."
            f"{dag.params[f'{dataset}_dataset']}.{dag.params[f'{table}_activity_table']}_check` "
            "WHERE run_date = '{{ ds }}' AND rel_change < -0.1",
            pass_value=0,
            task_id=f"task_{table}_{dataset}_check_change",
            dag=dag,
            location="EU",
            use_legacy_sql=False,
            depends_on_past=False,
            wait_for_downstream=False,
            on_failure_callback=callbacks.task_fail_slack_alert(
                "U03BANPLXJR", channel="prod"  # Carlos
            ),
        )
        check_table >> check_change_task

        start >> delete_job
        final_tasks[table] = check_change_task

        return check_change_task

    # Activity
    results_activity("Activity", "Place", "SG", "poi", "fk_sgplaces")
    results_activity("Activity", "ZipCode", "", "zipcode", "fk_zipcodes")

    return final_tasks
