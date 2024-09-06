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

    check_places_start = DummyOperator(
        task_id="check_places_start",
        dag=dag,
    )
    check_only_US_places = BigQueryValueCheckOperator(
gcp_conn_id="google_cloud_olvin_default",
        task_id="check_only_US_places",
        sql="""
            SELECT
                COUNT(DISTINCT iso_country_code)
            FROM
                `{{ params['project'] }}.{{ params['staging_data_dataset'] }}.sg_places`
        """,
        pass_value=1,
        dag=dag,
        location="EU",
        use_legacy_sql=False,
        depends_on_past=False,
        wait_for_downstream=False,
        retries=0,
        on_failure_callback=callbacks.task_fail_slack_alert(slack_users.MATIAS)
    )
    check_places_less_than_raw = BigQueryValueCheckOperator(
gcp_conn_id="google_cloud_olvin_default",
        task_id="check_places_less_than_raw",
        sql="""
            WITH
                places_count AS (
                    SELECT
                        COUNT(*) AS places
                    FROM
                        `{{ params['project'] }}.{{ params['staging_data_dataset'] }}.sg_places`
                ),
                raw_US_count AS (
                    SELECT
                        COUNT(*) AS raw
                    FROM
                        `{{ params['project'] }}.{{ params['staging_data_dataset'] }}.sg_raw`
                    WHERE
                        iso_country_code = 'US'
                )
            SELECT
                raw_US_count.raw > places_count.places
            FROM
                places_count,
                raw_US_count
        """,
        pass_value=True,
        dag=dag,
        location="EU",
        use_legacy_sql=False,
        depends_on_past=False,
        wait_for_downstream=False,
        retries=0,
        on_failure_callback=callbacks.task_fail_slack_alert(slack_users.MATIAS)
    )
    check_places_end = DummyOperator(
        task_id="check_places_end",
        dag=dag,
    )
    (
        start
        >> check_places_start
        >> [check_only_US_places, check_places_less_than_raw]
        >> check_places_end
    )

    return check_places_end
