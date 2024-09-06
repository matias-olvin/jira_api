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

    def bq_table_suffix(sep="") -> str:
        """
        Returns: latest table_id as string
        """
        from datetime import datetime

        year = f"{datetime.today().year}"

        if len(str(datetime.today().month)) == 1:
            month = f"0{datetime.today().month}"
        else:
            month = f"{datetime.today().month}"

        table_id = f"{year}{sep}{month}{sep}01"

        return table_id

    check_places_history_2_start = DummyOperator(
        task_id="check_places_history_2_start",
        dag=dag,
    )
    check_first_seen_nulls = BigQueryValueCheckOperator(
gcp_conn_id="google_cloud_olvin_default",
        task_id="check_first_seen_nulls_2",
        sql="""
            SELECT
                COUNT(first_seen)
            FROM
                `{{ params['project'] }}.{{ params['staging_data_dataset'] }}.{{ params['all_places_table'] }}`
            WHERE first_seen IS NULL
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
    check_last_seen_nulls = BigQueryValueCheckOperator(
gcp_conn_id="google_cloud_olvin_default",
        task_id="check_last_seen_nulls_2",
        sql="""
            SELECT
                COUNT(last_seen)
            FROM
                `{{ params['project'] }}.{{ params['staging_data_dataset'] }}.{{ params['all_places_table'] }}`
            WHERE last_seen IS NULL
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
    check_last_seen_count = BigQueryValueCheckOperator(
gcp_conn_id="google_cloud_olvin_default",
        task_id="check_last_seen_count_2",
        sql=f"""
            WITH
                last_seen_count AS (
                    SELECT
                        COUNT(*) AS total
                    FROM
                        `{{{{ params['project'] }}}}.{{{{ params['staging_data_dataset'] }}}}.{{{{ params['all_places_table'] }}}}`
                    GROUP BY
                        last_seen
                    HAVING
                        last_seen = '{bq_table_suffix(sep='-')}'
                ),
                places_count AS (
                    SELECT
                        COUNT(*) AS total
                    FROM
                        `{{{{ params['project'] }}}}.{{{{ params['staging_data_dataset'] }}}}.sg_places`
                )
            SELECT
                last_seen_count.total - places_count.total
            FROM
                last_seen_count,
                places_count
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
    check_sg_id_duplicates = BigQueryValueCheckOperator(
gcp_conn_id="google_cloud_olvin_default",
        task_id="check_sg_id_duplicates",
        sql="""
            SELECT COUNT(*)
            FROM (
                SELECT
                    COUNT(*) AS sg_id_count
                FROM
                    `{{ params['project'] }}.{{ params['staging_data_dataset'] }}.{{ params['all_places_table'] }}`
                GROUP BY
                    sg_id
            )
            WHERE
                sg_id_count > 1
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
    # check_olvin_id_equals_sg_id_active = BigQueryValueCheckOperator(
# gcp_conn_id="google_cloud_olvin_default",
    #     task_id="check_olvin_id_equals_sg_id_active",
    #     sql="""
    #         SELECT
    #             COUNT(DISTINCT olvin_id) = COUNT(DISTINCT sg_id_active)
    #         FROM
    #             `{{ params['project'] }}.{{ params['places_data_dataset'] }}.{{ params['all_places_table'] }}`
    #         WHERE
    #             active IS TRUE
    #     """,
    #     pass_value=True,
    #     labels={
    #         "pipeline": "{{ dag.dag_id }}", "task_id": "{{ task.task_id.lower()[:63] }}"
    #     },
    #     project_id=Variable.get("compute_project_id"),
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
    check_olvin_id_nulls = BigQueryValueCheckOperator(
gcp_conn_id="google_cloud_olvin_default",
        task_id="check_olvin_id_nulls",
        sql="""
            SELECT COUNT(*)
            FROM (
                SELECT olvin_id
                FROM
                    `{{ params['project'] }}.{{ params['staging_data_dataset'] }}.{{ params['all_places_table'] }}`
                WHERE
                    olvin_id IS NULL
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
    # check_olvin_id_equal_to_sg_id_first = BigQueryValueCheckOperator(
# gcp_conn_id="google_cloud_olvin_default",
    #     task_id="check_olvin_id_equal_to_sg_id_first",
    #     sql="""
    #         SELECT
    #             COUNT(DISTINCT olvin_id) = COUNT(DISTINCT sg_id_first)
    #         FROM
    #             `{{ params['project'] }}.{{ params['places_data_dataset'] }}.{{ params['all_places_table'] }}`
    #         WHERE
    #             active IS TRUE
    #     """,
    #     pass_value=True,
    #     labels={
    #         "pipeline": "{{ dag.dag_id }}", "task_id": "{{ task.task_id.lower()[:63] }}"
    #     },
    #     project_id=Variable.get("compute_project_id"),
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
    # check_sg_id_active_equal_to_sg_id_first = BigQueryValueCheckOperator(
# gcp_conn_id="google_cloud_olvin_default",
    #     task_id="check_sg_id_active_equal_to_sg_id_first",
    #     sql="""
    #         SELECT
    #             COUNT(DISTINCT sg_id_active) = COUNT(DISTINCT sg_id_first)
    #         FROM
    #             `{{ params['project'] }}.{{ params['places_data_dataset'] }}.{{ params['all_places_table'] }}`
    #         WHERE
    #             active IS TRUE
    #     """,
    #     pass_value=True,
    #     labels={
    #         "pipeline": "{{ dag.dag_id }}", "task_id": "{{ task.task_id.lower()[:63] }}"
    #     },
    #     project_id=Variable.get("compute_project_id"),
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
    check_places_history_2_end = DummyOperator(
        task_id="check_places_history_2_end",
        dag=dag,
    )
    (
        start
        >> check_places_history_2_start
        >> check_first_seen_nulls
        >> check_last_seen_nulls
        >> check_last_seen_count
        >> check_places_history_2_end
    )
    (
        start
        >> check_places_history_2_start
        >> check_sg_id_duplicates
        >> check_olvin_id_nulls
        >> check_places_history_2_end
    )
    # (
    #     start
    #     >> check_places_history_2_start
    #     >> check_olvin_id_equals_sg_id_active
    #     >> check_olvin_id_equal_to_sg_id_first
    #     >> check_sg_id_active_equal_to_sg_id_first
    #     >> check_places_history_2_end
    # )

    return check_places_history_2_end
