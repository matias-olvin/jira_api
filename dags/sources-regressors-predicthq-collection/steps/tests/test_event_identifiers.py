"""
DAG ID: group_visits_regressors
"""
from common.utils import callbacks
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryValueCheckOperator,
)


def register(dag, start):
    """Register tasks on the dag.

    Args:
        dag (airflow.models.DAG): DAG instance to register tasks on.
        start (airflow.models.TaskInstance): Task instance all tasks will
            registered downstream from.

    Returns:
        airflow.models.TaskInstance: The last task node in this section.
    """
    tests_start = DummyOperator(task_id="event_identifiers_test_start", dag=dag)
    tests_end = DummyOperator(
        task_id="event_identifiers_test_end", dag=dag, trigger_rule="all_done"
    )

    EVENT_TYPES = [
        "expo",
        "festivals",
        "conferences",
        "sports",
        "performing-arts",
        "community",
        "concerts",
    ]

    for event_type in EVENT_TYPES:
        test_event_identifier_count = BigQueryInsertJobOperator(
            task_id=f"test_{event_type}_count",
            configuration={
                "query": {
                    "query": f"""
                        DECLARE event_type DEFAULT '{event_type}';
                        {{% include './bigquery/tests/test_event_identifiers.sql' %}}
                    """,
                    "useLegacySql": False,
                },
                "labels": {
                    "dag_id": "{{ dag.dag_id }}",
                    "task_id": "{{ task.task_id.lower()[:63] }}",
                },
            },
        )
        check_event_identifier_count = BigQueryValueCheckOperator(
gcp_conn_id="google_cloud_olvin_default",
            task_id=f"check_{event_type}_count",
            sql=f"""
                SELECT
                    result
                FROM
                    `{{{{ var.value.env_project }}}}.{{{{ params['test_compilation_dataset'] }}}}.{{{{ params['test_results_table'] }}}}`
                WHERE
                    regressor = 'regressors_collection_predicthq_events' AND
                    test_name = '{event_type} count' AND
                    run_date = '{{{{ ds }}}}'
            """,
            pass_value=True,
            dag=dag,
            location="EU",
            use_legacy_sql=False,
            retries=0,
            on_failure_callback=callbacks.task_fail_slack_alert(
                "U034YDXAD1R",  # Jake
            ),
        )
        (
            start
            >> tests_start
            >> test_event_identifier_count
            >> check_event_identifier_count
            >> tests_end
        )

    return tests_end
