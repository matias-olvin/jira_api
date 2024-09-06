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

    deactivate_blacklist_pois_end = DummyOperator(task_id=f"deactivate_blacklist_pois_end")

    deactivate_blacklist_pois = BigQueryInsertJobOperator(
        gcp_conn_id="google_cloud_olvin_default",
        task_id=f"deactivate_blacklist_pois",
        configuration={
            "query": {
                "query": '{% include "./bigquery/deactivate_blacklist_pois.sql" %}',
                "useLegacySql": "False",
            }
        },
        dag=dag,
        location="EU",
    )

    check_activity_after_deactivating = BigQueryValueCheckOperator(
        gcp_conn_id="google_cloud_olvin_default",
        sql=f'{{% include "./bigquery/check_activity_after_deactivating.sql" %}}',
        pass_value=0,
        task_id=f"check_activity_after_deactivating",
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

    start >> deactivate_blacklist_pois >> check_activity_after_deactivating >> deactivate_blacklist_pois_end

    return deactivate_blacklist_pois_end