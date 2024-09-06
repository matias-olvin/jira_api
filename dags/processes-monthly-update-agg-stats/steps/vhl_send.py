"""
DAG ID: sg_agg_stats
"""

from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryValueCheckOperator,
)


def register(dag, start, mode):
    """Register tasks on the dag.

    Args:
        dag (airflow.models.DAG): DAG instance to register tasks on.
        start (airflow.models.TaskInstance): Task instance all tasks will
            registered downstream from.

    Returns:
        airflow.models.TaskInstance: The last task node in this section.
    """

    def results_vhl(dataset: str, table: str):
        database_table = f"SG{dataset}{table}"
        # Compute results
        query_job = BigQueryInsertJobOperator(
            task_id=f"query_{table}_{dataset}_send",
            project_id=dag.params["project"],
            configuration={
                "query": {
                    "query": f"{{% with "
                    f"database_table='{database_table}'"
                    f"%}}{{% include './bigquery/{dataset}{table}_send.sql' %}}{{% endwith %}}",
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
        start >> query_job

        check_duplicates_task = BigQueryValueCheckOperator(
gcp_conn_id="google_cloud_olvin_default",
            sql=f"""
                SELECT COUNT(*)
                FROM (
                    SELECT local_date,
                    FROM
                        `{{{{ params['project'] }}}}.{{{{ params['postgres_dataset'] }}}}.{database_table}`
                    GROUP BY local_date, fk_sgplaces
                    HAVING COUNT(*) > 1
                )
            """,
            pass_value=0,
            task_id=f"{table}_{dataset}_check_duplicates",
            dag=dag,
            location="EU",
            use_legacy_sql=False,
            depends_on_past=False,
            wait_for_downstream=False,
        )
        check_all_months_task = BigQueryValueCheckOperator(
gcp_conn_id="google_cloud_olvin_default",
            sql=f"""
                SELECT COUNT(*)
                FROM (
                    SELECT month
                    FROM UNNEST(
                        GENERATE_DATE_ARRAY(
                            DATE('2019-01-01'),
                            "{{{{ activity_param_1(execution_date) }}}}",
                            INTERVAL 1 MONTH
                        )
                    ) as month
                ) as date_table
                WHERE NOT EXISTS (
                    SELECT local_date
                    FROM (
                        SELECT local_date
                        FROM
                        `{{{{ params['project'] }}}}.{{{{ params['postgres_dataset'] }}}}.{database_table}`
                        GROUP BY local_date
                    ) AS dates_found
                    WHERE date_table.month=dates_found.local_date
                )
            """,
            pass_value=0,
            task_id=f"{table}_{dataset}_check_all_months",
            dag=dag,
            location="EU",
            use_legacy_sql=False,
            depends_on_past=False,
            wait_for_downstream=False,
        )
        check_average_task = BigQueryValueCheckOperator(
gcp_conn_id="google_cloud_olvin_default",
            sql=f"""
                SELECT COUNT(*)
                FROM (
                    SELECT local_date, total_month/AVG(total_month) OVER () -1 AS change_avg
                    FROM (
                        SELECT  local_date, COUNT(*) AS total_month
                        FROM
                        `{{{{ params['project'] }}}}.{{{{ params['postgres_dataset'] }}}}.{database_table}`
                        GROUP BY local_date
                        )
                    )
                WHERE change_avg < -0.17 AND (local_date >= "2021-01-01")
            """,
            pass_value=0,
            task_id=f"{table}_{dataset}_check_average",
            dag=dag,
            location="EU",
            use_legacy_sql=False,
            depends_on_past=False,
            wait_for_downstream=False,
        )

        query_job >> [check_duplicates_task, check_all_months_task, check_average_task]

        return [check_duplicates_task, check_all_months_task, check_average_task]

    # Visitor Home Location
    results_vhl_tasks = results_vhl("PlaceHome", "ZipCode")

    results_vhl_end = DummyOperator(task_id="vhl_send_end")
    results_vhl_tasks >> results_vhl_end

    return results_vhl_end
