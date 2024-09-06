from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator, BigQueryValueCheckOperator


def register(dag, start):
    """
    Register tasks on the dag.

    Args:
        dag (airflow.models.DAG): DAG instance to register tasks on.
        start (airflow.models.TaskInstance): Task instance all tasks will
            registered downstream from.

    Returns:
        airflow.models.TaskInstance: The last task node in this section.
    """

    checks_after_deactivation_end = DummyOperator(task_id=f"checks_after_deactivation_end")



    check_cityraw_after_deactivation = BigQueryValueCheckOperator(
        gcp_conn_id="google_cloud_olvin_default",
        sql=f'{{% include "./bigquery/checks_after_deactivation/check_cityraw_after_deactivation.sql" %}}',
        pass_value=0,
        task_id=f"check_cityraw_after_deactivation",
        dag=dag,
        location="EU",
        use_legacy_sql=False,
        depends_on_past=False,
        wait_for_downstream=False,
        # on_failure_callback=utils.task_fail_slack_alert(
        #     "U03BANPLXJR",  # Carlos
        #     channel="prod"
        # )
    )

    check_zipcoderaw_after_deactivation = BigQueryValueCheckOperator(
        gcp_conn_id="google_cloud_olvin_default",
        sql=f'{{% include "./bigquery/checks_after_deactivation/check_zipcoderaw_after_deactivation.sql" %}}',
        pass_value=0,
        task_id=f"check_zipcoderaw_after_deactivation",
        dag=dag,
        location="EU",
        use_legacy_sql=False,
        depends_on_past=False,
        wait_for_downstream=False,
        # on_failure_callback=utils.task_fail_slack_alert(
        #     "U03BANPLXJR",  # Carlos
        #     channel="prod"
        # )
    )

    check_sgcenterraw_after_deactivation = BigQueryValueCheckOperator(
        gcp_conn_id="google_cloud_olvin_default",
        sql=f'{{% include "./bigquery/checks_after_deactivation/check_sgcenterraw_after_deactivation.sql" %}}',
        pass_value=0,
        task_id=f"check_sgcenterraw_after_deactivation",
        dag=dag,
        location="EU",
        use_legacy_sql=False,
        depends_on_past=False,
        wait_for_downstream=False,
        # on_failure_callback=utils.task_fail_slack_alert(
        #     "U03BANPLXJR",  # Carlos
        #     channel="prod"
        # )
    )

    start >> check_cityraw_after_deactivation >> checks_after_deactivation_end
    start >> check_zipcoderaw_after_deactivation >> checks_after_deactivation_end
    start >> check_sgcenterraw_after_deactivation >> checks_after_deactivation_end

    return checks_after_deactivation_end