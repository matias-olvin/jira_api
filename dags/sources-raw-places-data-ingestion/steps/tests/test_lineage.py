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

    check_lineage_start = DummyOperator(
        task_id="check_lineage_start",
        dag=dag,
    )
    check_lineage_pks_duplicates = BigQueryValueCheckOperator(
gcp_conn_id="google_cloud_olvin_default",
        task_id="check_lineage_pks_duplicates",
        sql="""
            SELECT
                COUNT(*) - COUNT(DISTINCT placekey)
            FROM
                `{{ params['project'] }}.{{ params['staging_data_dataset'] }}.lineage`
        """,
        pass_value=0,
        dag=dag,
        location="EU",
        use_legacy_sql=False,
        depends_on_past=False,
        wait_for_downstream=False,
        retries=0,
        on_failure_callback=callbacks.task_fail_slack_alert(slack_users.MATIAS),
    )
    # check_lineage_rows = BigQueryValueCheckOperator(
# gcp_conn_id="google_cloud_olvin_default",
    #     task_id="check_lineage_rows",
    #     sql=f"""
    #         WITH
    #             total_rows_in AS (
    #                 SELECT row_count as rows_in
    #                 FROM
    #                     `{{{{ params['project'] }}}}.{{{{ params['staging_data_dataset'] }}}}.__TABLES__`
    #                 WHERE
    #                     table_id = '{{{{ params['all_places_table'] }}}}'
    #             ),
    #             total_rows_out AS (
    #                 SELECT row_count as rows_out
    #                 FROM
    #                     `{{{{ params['project'] }}}}.{{{{ params['staging_data_dataset'] }}}}.__TABLES__`
    #                 WHERE
    #                     table_id = 'lineage'
    #             )
    #         SELECT
    #             total_rows_in.rows_in - total_rows_out.rows_out
    #         FROM
    #             total_rows_in,
    #             total_rows_out
    #     """,
    #     pass_value=0,
    #     dag=dag,
    #     location="EU",
    #     use_legacy_sql=False,
    #     depends_on_past=False,
    #     wait_for_downstream=False,
    #     retries=0,
    #     on_failure_callback=callbacks.task_fail_slack_alert(
    #         "U034YDXAD1R",
    #         "U03BANPLXJR"
    #     ),
    # )
    check_lineage_end = DummyOperator(
        task_id="check_lineage_end",
        dag=dag,
    )
    (
        start
        >> check_lineage_start
        >> check_lineage_pks_duplicates
        # check_lineage_rows
        >> check_lineage_end
    )
    # (
    #     start
    #     >> check_lineage_start
    #     >> check_lineage_rows
    #     >> check_lineage_end
    # )

    return check_lineage_end
