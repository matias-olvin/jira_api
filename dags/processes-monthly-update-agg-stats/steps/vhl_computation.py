"""
DAG ID: sg_agg_stats
"""

from datetime import timedelta

from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


def register(dag, start, mode):
    """Register tasks on the dag.

    Args:
        dag (airflow.models.DAG): DAG instance to register tasks on.
        start (airflow.models.TaskInstance): Task instance all tasks will
            registered downstream from.

    Returns:
        airflow.models.TaskInstance: The last task node in this section.
    """
    final_tasks = []

    def results_vhl(dataset: str, table: str):
        if mode == "update":
            database_table = f"{dataset}{table}"
            home_table = (
                f"{dag.params['project']}.{dag.params[f'{dataset}_dataset']}."
                f"{dag.params[f'{table}_table']}"
            )
            delete_job = BigQueryInsertJobOperator(
                task_id=f"query_{table}_{dataset}_delete_initial",
                project_id=dag.params["project"],
                configuration={
                    "query": {
                        "query": f"DELETE FROM `{home_table}` "
                        f"WHERE local_date = '{{{{ activity_param_1(execution_date) }}}}'",
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
            database_table = f"{dataset}{table}"
            home_table = (
                f"{dag.params['project']}.{dag.params[f'{dataset}_dataset']}."
                f"{dag.params[f'{table}_table']}"
            )
            delete_job = BigQueryInsertJobOperator(
                task_id=f"query_{table}_{dataset}_delete_initial",
                project_id=dag.params["project"],
                configuration={
                    "query": {
                        "query": f"DELETE FROM `{home_table}` WHERE TRUE",
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
        start >> delete_job

        # Compute results
        query_job = BigQueryInsertJobOperator(
            task_id=f"query_{table}_{dataset}",
            project_id=dag.params["project"],
            retries=1,
            retry_delay=timedelta(hours=1),
            configuration={
                "query": {
                    "query": f"{{% include './bigquery/{database_table}.sql' %}}",
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
        delete_job >> query_job
        final_tasks.append(query_job)

    # Visitor Home Location
    results_vhl("PlaceHome", "ZipCode")

    results_vhl_end = DummyOperator(task_id="vhl_computation_end")
    final_tasks >> results_vhl_end

    return results_vhl_end
