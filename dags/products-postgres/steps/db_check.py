from airflow.models import DAG, TaskInstance
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.task_group import TaskGroup


def register(start: TaskInstance, dag: DAG) -> TaskGroup:
    def check_uniqueness(
        table_bigquery: str,
        database_table: str,
        fk_column: str,
        start: TaskInstance,
        end: TaskInstance,
        dataset_bigquery=dag.params["postgres_dataset"],
        date_column="local_date",
    ):
        checkUniqueness_job = BigQueryInsertJobOperator(
            gcp_conn_id="google_cloud_olvin_default",
            task_id=f"checkUniqueness_{database_table}",
            configuration={
                "query": {
                    "query": f"""
                        ASSERT (
                            SELECT count(*)
                            FROM `{dag.params['project']}.{dataset_bigquery}.{table_bigquery}`
                        ) = (
                            SELECT count(*)
                            FROM (
                                SELECT DISTINCT {date_column}, {fk_column}
                                FROM `{dag.params['project']}.{dataset_bigquery}.{table_bigquery}`
                            )
                        )
                    """,
                    "useLegacySql": "False",
                },
                "labels": {
                    "pipeline": "{{ dag.dag_id.lower()[:63] |  replace('.','-') }}",
                    "task_id": "{{ task.task_id.lower()[:63] |  replace('.','-') }}",
                },
            },
            dag=dag,
        )
        start >> checkUniqueness_job >> end

    def process_invalid_keys(
        dataset_bigquery: str,
        table_bigquery: str,
        database_table: str,
        fk_column: str,
        comparison_table: str,
        start: TaskInstance,
        end: TaskInstance,
    ):
        processInvalidKeys_job = BigQueryInsertJobOperator(
            gcp_conn_id="google_cloud_olvin_default",
            task_id=f"processInvalidKeys_{database_table}",
            configuration={
                "query": {
                    "query": f"""
                            ASSERT (
                                SELECT COUNT({fk_column}) as cnt
                                FROM `{dag.params['project']}.{dataset_bigquery}.{table_bigquery}` t1
                                WHERE NOT EXISTS (
                                    SELECT 1
                                    FROM `{dag.params['project']}.{dataset_bigquery}.{comparison_table}` t2
                                    WHERE t1.{fk_column} = t2.pid
                                )
                            ) = 0
                        """,
                    "useLegacySql": "False",
                },
                "labels": {
                    "pipeline": "{{ dag.dag_id.lower()[:63] |  replace('.','-') }}",
                    "task_id": "{{ task.task_id.lower()[:63] |  replace('.','-') }}",
                },
            },
            dag=dag,
        )
        start >> processInvalidKeys_job >> end

    with TaskGroup("db_check_task_group", tooltip="Check database integrity") as group:

        check_uniqueness_end = EmptyOperator(task_id="checkUniqueness_end")

        process_invalid_keys_end = EmptyOperator(task_id="processInvalidKeys_end")

        for table in dag.params["uniqueness_check_input_tables_list"]:
            check_uniqueness(
                table_bigquery=table,
                database_table=table,
                fk_column="fk_sgplaces",
                start=start,
                end=check_uniqueness_end,
            )

        # process invalid keys
        process_invalid_keys(
            "{{ params['postgres_batch_dataset'] }}",
            "SGPlaceRaw",
            "SGPlaceRaw",
            "fk_sgbrands",
            "SGBrandRaw",
            check_uniqueness_end,
            process_invalid_keys_end,
        )
        process_invalid_keys(
            "{{ params['postgres_batch_dataset'] }}",
            "SGPlaceActivity",
            "SGPlaceActivity",
            "fk_sgplaces",
            "SGPlaceRaw",
            check_uniqueness_end,
            process_invalid_keys_end,
        )
        process_invalid_keys(
            "{{ params['postgres_batch_dataset'] }}",
            "ZipCodeActivity",
            "ZipCodeActivity",
            "fk_zipcodes",
            "ZipCodeRaw",
            check_uniqueness_end,
            process_invalid_keys_end,
        )
        process_invalid_keys(
            "{{ params['postgres_batch_dataset'] }}",
            "SGPlaceHourlyVisitsRaw",
            "SGPlaceHourlyVisitsRaw",
            "fk_sgplaces",
            "SGPlaceRaw",
            check_uniqueness_end,
            process_invalid_keys_end,
        )
        process_invalid_keys(
            "{{ params['postgres_batch_dataset'] }}",
            "SGPlaceDailyVisitsRaw",
            "SGPlaceDailyVisitsRaw",
            "fk_sgplaces",
            "SGPlaceRaw",
            check_uniqueness_end,
            process_invalid_keys_end,
        )
        process_invalid_keys(
            "{{ params['postgres_batch_dataset'] }}",
            "SGPlaceMonthlyVisitsRaw",
            "SGPlaceMonthlyVisitsRaw",
            "fk_sgplaces",
            "SGPlaceRaw",
            check_uniqueness_end,
            process_invalid_keys_end,
        )
        process_invalid_keys(
            "{{ params['postgres_batch_dataset'] }}",
            "SGPlaceRanking",
            "SGPlaceRanking",
            "fk_sgplaces",
            "SGPlaceRaw",
            check_uniqueness_end,
            process_invalid_keys_end,
        )
        process_invalid_keys(
            "{{ params['postgres_batch_dataset'] }}",
            "BrandMallLocations",
            "BrandMallLocations",
            "fk_sgbrands",
            "SGBrandRaw",
            check_uniqueness_end,
            process_invalid_keys_end,
        )

    return group
