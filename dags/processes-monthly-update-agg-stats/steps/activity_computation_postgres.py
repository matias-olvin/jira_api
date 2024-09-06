"""
DAG ID: sg_agg_stats
"""
from typing import Dict

from airflow import DAG
from airflow.models import TaskInstance
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.sensors.external_task import ExternalTaskMarker


def register(dag: DAG, start: Dict[str, TaskInstance], mode: str):
    """Register tasks on the dag.

    Args:
        dag (airflow.models.DAG): DAG instance to register tasks on.
        start (airflow.models.TaskInstance): Task instance all tasks will
            registered downstream from.
        mode:

    Returns:
        airflow.models.TaskInstance: The last task node in this section.
    """

    final_tasks = {}

    def results_activity_postgres(
        dataset: str, table: str, insert: str, location: str, location_key: str
    ):
        # with TaskGroup(group_id=f"group_copy_{table}_{dataset}") as group_activity:

        database_table = f"{insert}{table}{dataset}"
        if mode == "update":
            if location == "poi":
                query_copy_final_job = BigQueryInsertJobOperator(
                    task_id=f"query_copy_final_{table}_{dataset}",
                    project_id=dag.params["project"],
                    configuration={
                        "query": {
                            "query": f"CREATE OR REPLACE TABLE {{{{ params['project'] }}}}."
                            f"{{{{ params['postgres_dataset'] }}}}.{database_table} AS "
                            f"SELECT {location_key}, "
                            "CASE "
                            f" WHEN activity = 'no_data' THEN 'no_data' "
                            " WHEN activity = 'active' THEN 'active' "
                            " WHEN activity = 'watch_list' THEN 'active' "
                            " WHEN activity = 'inactive' THEN 'limited_data' "
                            "END AS activity, "
                            f"confidence_level, "
                            f"home_location_data AS home_locations, networks_data AS connections "
                            f"FROM `{{{{ params['project'] }}}}."
                            f"{{{{ params['{dataset}_dataset'] }}}}."
                            f"{{{{ params['{table}_activity_table'] }}}}` "
                            f"WHERE run_date='{{{{ ds }}}}'",
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

                propagate_activity = ExternalTaskMarker(
                    task_id=f"propagate_activity_{table}_{dataset}",
                    external_dag_id="processes-monthly-update-networks-backfill",
                    external_task_id="wait_for_activity",
                    dag=dag,
                    execution_date="{{ ds }}",
                )
                query_copy_final_job >> propagate_activity

            elif location == "zipcode":
                query_copy_final_job = BigQueryInsertJobOperator(
                    task_id=f"query_copy_final_{table}_{dataset}",
                    project_id=dag.params["project"],
                    configuration={
                        "query": {
                            "query": f"CREATE OR REPLACE TABLE {{{{ params['project'] }}}}."
                            f"{{{{ params['postgres_dataset'] }}}}.{database_table} AS "
                            f"SELECT {location_key}, "
                            "CASE "
                            " WHEN activity = 'active' THEN 'active' "
                            " WHEN activity = 'watch_list' THEN 'active' "
                            " WHEN activity = 'inactive' THEN 'limited_data' "
                            " WHEN activity = 'no_data' THEN 'no_data' "
                            "END AS activity, "
                            f"confidence_level, "
                            f"FROM `{{{{ params['project'] }}}}."
                            f"{{{{ params['{dataset}_dataset'] }}}}."
                            f"{{{{ params['{table}_activity_table'] }}}}`"
                            f"WHERE run_date='{{{{ ds }}}}'",
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
        elif mode == "backfill":
            if location == "poi":
                query_copy_final_job = BigQueryInsertJobOperator(
                    task_id=f"query_copy_final_{table}_{dataset}",
                    project_id=dag.params["project"],
                    configuration={
                        "query": {
                            "query": f"CREATE OR REPLACE TABLE {{{{ params['project'] }}}}."
                            f"{{{{ params['postgres_dataset'] }}}}.{database_table} AS "
                            f"SELECT {location_key}, "
                            "CASE "
                            f" WHEN activity = 'no_data' THEN 'no_data' "
                            " WHEN activity = 'active' THEN 'active' "
                            " WHEN activity = 'watch_list' THEN 'active' "
                            " WHEN activity = 'inactive' THEN 'limited_data' "
                            "END AS activity, "
                            f"confidence_level, "
                            f"home_location_data AS home_locations, networks_data AS connections "
                            f"FROM `{{{{ params['project'] }}}}."
                            f"{{{{ params['{dataset}_dataset'] }}}}."
                            f"{{{{ params['{table}_activity_table'] }}}}` "
                            f"WHERE run_date='{{{{ ds }}}}'",
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

            elif location == "zipcode":
                query_copy_final_job = BigQueryInsertJobOperator(
                    task_id=f"query_copy_final_{table}_{dataset}",
                    project_id=dag.params["project"],
                    configuration={
                        "query": {
                            "query": f"CREATE OR REPLACE TABLE {{{{ params['project'] }}}}."
                            f"{{{{ params['postgres_dataset'] }}}}.{database_table} AS "
                            f"SELECT {location_key}, "
                            "CASE "
                            " WHEN activity = 'active' THEN 'active' "
                            " WHEN activity = 'watch_list' THEN 'active' "
                            " WHEN activity = 'inactive' THEN 'limited_data' "
                            " WHEN activity = 'no_data' THEN 'no_data' "
                            "END AS activity, "
                            f"confidence_level, "
                            f"FROM `{{{{ params['project'] }}}}."
                            f"{{{{ params['{dataset}_dataset'] }}}}."
                            f"{{{{ params['{table}_activity_table'] }}}}` "
                            f"WHERE run_date='{{{{ ds }}}}'",
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

        start[table] >> query_copy_final_job
        final_tasks[table] = query_copy_final_job

        return query_copy_final_job

    results_activity_postgres("Activity", "Place", "SG", "poi", "fk_sgplaces")
    results_activity_postgres("Activity", "ZipCode", "", "zipcode", "fk_zipcodes")

    return final_tasks
