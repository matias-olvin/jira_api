"""
DAG ID: dynamic_places
"""
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryValueCheckOperator
from common.utils import callbacks, slack_users

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

    check_raw_data_start = DummyOperator(
        task_id="check_raw_data_start",
        dag=dag,
    )
    check_raw_placekey_duplicates = BigQueryValueCheckOperator(
gcp_conn_id="google_cloud_olvin_default",
        task_id="check_raw_placekey_duplicates",
        sql="""
            SELECT COUNT(*)
            FROM (
                SELECT
                    COUNT(*) AS placekey_count
                FROM
                    `{{ params['project'] }}.{{ params['staging_data_dataset'] }}.sg_raw`
                GROUP BY
                    placekey
            )
            WHERE
                placekey_count > 1
        """,
        pass_value=0,
        dag=dag,
        location="EU",
        use_legacy_sql=False,
        depends_on_past=False,
        wait_for_downstream=False,
        retries=0,
        on_failure_callback=callbacks.task_fail_slack_alert(slack_users.MATIAS)
    )
    check_raw_placekey_nulls = BigQueryValueCheckOperator(
gcp_conn_id="google_cloud_olvin_default",
        task_id="check_raw_placekey_nulls",
        sql="""
            SELECT COUNT(*)
            FROM (
                SELECT placekey
                FROM
                    `{{ params['project'] }}.{{ params['staging_data_dataset'] }}.sg_raw`
                WHERE
                    placekey IS NULL
            )
        """,
        pass_value=0,
        dag=dag,
        location="EU",
        use_legacy_sql=False,
        depends_on_past=False,
        wait_for_downstream=False,
        retries=0,
        on_failure_callback=callbacks.task_fail_slack_alert(slack_users.MATIAS)
    )
    check_raw_polygon_nulls = BigQueryValueCheckOperator(
gcp_conn_id="google_cloud_olvin_default",
        task_id="check_raw_polygon_nulls",
        sql="""
            SELECT
                COUNT(*)
            FROM
                `{{ params['project'] }}.{{ params['staging_data_dataset'] }}.sg_raw`
            WHERE
                (geometry_type IS NULL) OR
                (polygon_wkt IS NULL)
        """,
        pass_value=0,
        dag=dag,
        location="EU",
        use_legacy_sql=False,
        depends_on_past=False,
        wait_for_downstream=False,
        retries=0,
        on_failure_callback=callbacks.task_fail_slack_alert(slack_users.MATIAS)
    )
    check_raw_data_end = DummyOperator(
        task_id="check_raw_data_end",
        dag=dag,
    )
    (
        start
        >> check_raw_data_start
        >> [
            check_raw_placekey_duplicates,
            check_raw_placekey_nulls,
            check_raw_polygon_nulls,
        ]
        >> check_raw_data_end
    )

    return check_raw_data_end
