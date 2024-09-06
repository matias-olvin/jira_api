"""
DAG ID: dynamic_places
"""
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryValueCheckOperator,
)


def register(dag, start):
    """
    Register tasks on the dag

    Args:
        dag (airflow.models.DAG): DAG instance to register tasks on.
        start (airflow.models.TaskInstance): Task instance all tasks will
        be registered downstream from.

    Returns:
        airflow.models.TaskInstance: The last task node in this section.
    """

    check_ideal_logic_start = DummyOperator(
        task_id="check_ideal_logic_start",
        dag=dag,
    )
    create_ideal_logic_places_dynamic = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="create_ideal_logic_places_dynamic",
        configuration={
            "query": {
                "query": "{% include './bigquery/ideal_places_dynamic.sql' %}",
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
    check_ideal_logic_olvin_id_duplicates = BigQueryValueCheckOperator(
gcp_conn_id="google_cloud_olvin_default",
        task_id="check_ideal_logic_olvin_id_duplicates",
        sql="""
            SELECT COUNT(*)
            FROM (
                SELECT
                    COUNT(*) AS pid_count
                FROM
                    `{{ params['project'] }}.{{ params['staging_data_dataset'] }}.{{ params['ideal_dynamic_places_table'] }}`
                GROUP BY
                    pid
            )
            WHERE
                pid_count > 1
        """,
        pass_value=0,
        dag=dag,
        location="EU",
        use_legacy_sql=False,
        depends_on_past=False,
        wait_for_downstream=False,
        retries=0,
        # on_failure_callback=utils.task_fail_slack_alert(
        #     "U034YDXAD1R",
        #     "U03BANPLXJR"
        # ),
    )
    check_ideal_logic_olvin_id_nulls = BigQueryValueCheckOperator(
gcp_conn_id="google_cloud_olvin_default",
        task_id="check_ideal_logic_olvin_id_nulls",
        sql="""
            SELECT COUNT(*)
            FROM (
                SELECT pid
                FROM
                    `{{ params['project'] }}.{{ params['staging_data_dataset'] }}.{{ params['ideal_dynamic_places_table'] }}`
                WHERE
                    pid IS NULL
            )
        """,
        pass_value=0,
        dag=dag,
        location="EU",
        use_legacy_sql=False,
        depends_on_past=False,
        wait_for_downstream=False,
        retries=0,
        # on_failure_callback=utils.task_fail_slack_alert(
        #     "U034YDXAD1R",
        #     "U03BANPLXJR"
        # ),
    )
    check_ideal_logic_end = DummyOperator(
        task_id="check_ideal_logic_end", dag=dag, trigger_rule="all_done"
    )
    (
        start
        >> check_ideal_logic_start
        >> create_ideal_logic_places_dynamic
        >> [check_ideal_logic_olvin_id_nulls, check_ideal_logic_olvin_id_duplicates]
        >> check_ideal_logic_end
    )

    return check_ideal_logic_end
