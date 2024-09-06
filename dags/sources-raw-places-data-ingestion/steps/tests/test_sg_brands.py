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

    check_brands_start = DummyOperator(
        task_id="check_brands_start",
        dag=dag,
    )
    check_brand_id_duplicates = BigQueryValueCheckOperator(
gcp_conn_id="google_cloud_olvin_default",
        task_id="check_brand_id_duplicates",
        sql="""
            SELECT COUNT(*)
            FROM (
                SELECT
                    COUNT(*) AS brand_count
                FROM
                    `{{ params['project'] }}.{{ params['staging_data_dataset'] }}.sg_brands`
                GROUP BY
                    safegraph_brand_id
            )
            WHERE
                brand_count > 1
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
    check_brand_id_nulls = BigQueryValueCheckOperator(
gcp_conn_id="google_cloud_olvin_default",
        task_id="check_brand_id_nulls",
        sql="""
            SELECT COUNT(*)
            FROM (
                SELECT safegraph_brand_id
                FROM
                    `{{ params['project'] }}.{{ params['staging_data_dataset'] }}.sg_brands`
                WHERE
                    safegraph_brand_id IS NULL
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
    check_brands_end = DummyOperator(
        task_id="check_brands_end",
        dag=dag,
    )
    (
        start
        >> check_brands_start
        >> [check_brand_id_duplicates, check_brand_id_nulls]
        >> check_brands_end
    )

    return check_brands_end
