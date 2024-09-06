from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator, BigQueryValueCheckOperator


def register(dag, start, postgres_dataset):
    """
    Register tasks on the dag.

    Args:
        dag (airflow.models.DAG): DAG instance to register tasks on.
        start (airflow.models.TaskInstance): Task instance all tasks will
            registered downstream from.

    Returns:
        airflow.models.TaskInstance: The last task node in this section.
    """

    update_places_benchmark_table_end = DummyOperator(task_id=f"update_{postgres_dataset}_places_benchmark_end")

    clear_SGPlaceBenchmarkingRaw_table = BigQueryInsertJobOperator(
        gcp_conn_id="google_cloud_olvin_default",
        task_id=f"clear_{postgres_dataset}_SGPlaceBenchmarkingRaw_table",
        configuration={
            "query": {
                "query": (
                    '{% with postgres_dataset="'
                    f"{postgres_dataset}"
                    '"%}{% include "./bigquery/sgplacebenchmark/clear_sgplacebenchmark.sql" %}{% endwith %}'
                ),
                "useLegacySql": "False",
            }
        },
        dag=dag,
        location="EU",
    )

    insert_SGPlaceBenchmarkingRaw_table = BigQueryInsertJobOperator(
        gcp_conn_id="google_cloud_olvin_default",
        task_id=f"insert_{postgres_dataset}_SGPlaceBenchmarkingRaw_table",
        configuration={
            "query": {
                "query": (
                    '{% with postgres_dataset="'
                    f"{postgres_dataset}"
                    '"%}{% include "./bigquery/sgplacebenchmark/insert_sgplacebenchmark.sql" %}{% endwith %}'
                ),
                "useLegacySql": "False",
            }
        },
        dag=dag,
        location="EU",
    )

    check_perc_pois_with_benchmark = BigQueryValueCheckOperator(
        gcp_conn_id="google_cloud_olvin_default",
        sql=(
                '{% with postgres_dataset="'
                f"{postgres_dataset}"
                '"%}{% include "./bigquery/sgplacebenchmark/check_perc_benchmark.sql" %}{% endwith %}'
            ),
        pass_value=True,
        task_id=f"check_perc_pois_with_benchmark_{postgres_dataset}",
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

    check_increase_wrt_latest_month = BigQueryValueCheckOperator(
        gcp_conn_id="google_cloud_olvin_default",
        sql=(
                '{% with postgres_dataset="'
                f"{postgres_dataset}"
                '"%}{% include "./bigquery/sgplacebenchmark/check_increase_wrt_latest_month.sql" %}{% endwith %}'
            ),
        pass_value=True,
        task_id=f"check_increase_wrt_latest_month_{postgres_dataset}",
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

    start >> clear_SGPlaceBenchmarkingRaw_table >> insert_SGPlaceBenchmarkingRaw_table >> \
    [check_perc_pois_with_benchmark, check_increase_wrt_latest_month] >> update_places_benchmark_table_end

    return update_places_benchmark_table_end