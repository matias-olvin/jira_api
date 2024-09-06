"""
DAG ID: dynamic_places
"""
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryDeleteTableOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.sensors.gcs import (
    GCSObjectsWithPrefixExistenceSensor,
)


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
    from common.utils import callbacks

    query_dict = {
        "spend_patterns": "{% include './bigquery/append_spend_patterns.sql' %}",
        # "transaction_panel_summary": "{% include './bigquery/spend/append_transaction_panel_summary.sql' %}"
    }
    table_dict = {
        "spend_patterns": dag.params["spend_patterns_table"],
        # "transaction_panel_summary": dag.params['transaction_panel_summary_table']
    }

    def wait_for_raw(data, dag=dag):
        gcs_sensor = GCSObjectsWithPrefixExistenceSensor(
            task_id=f"wait_for_sg_raw_{data}",
            bucket=f"{dag.params['sg_data_bucket']}",
            prefix="{{ params['sg_spend_data_blobs'] }}/{{ data_interval_end.strftime('%Y') }}/{{ data_interval_end.strftime('%m') }}",
            dag=dag,
            poke_interval=24 * 60 * 60,
            timeout=5 * 24 * 60 * 60,
            soft_fail=False,
            mode="reschedule",
            on_success_callback=callbacks.task_end_custom_slack_alert(
                "U034YDXAD1R",
                msg_title="monthly spend data found in safegraph-raw-data-olvin-com - pipeline started.",
            ),
        )

        return gcs_sensor

    wait_for_spend_patterns = wait_for_raw(data="spend_patterns")
    # wait_for_transaction_panel_summary = wait_for_raw(data="transaction_panel_summary")

    def load_data(data: str, dag=dag):
        """
        Operates similar to BigQueryInsertJobOperator
        With 'load' configuration
        Using bigquery API allows dynanmic sourceUri
        """

        def bq_load_sg_spend_job(data: str, dag=dag, **context):
            import logging

            from google.cloud import bigquery

            bq_client = bigquery.Client(project=Variable.get("storage_project_id"))
            # set table_id to the ID of the table to create.
            table_id = f"{dag.params['storage_project_id']}.{dag.params['staging_data_dataset']}.{data}"

            job_config = bigquery.LoadJobConfig()
            job_config.write_disposition = "WRITE_TRUNCATE"
            job_config.create_disposition = "CREATE_IF_NEEDED"
            job_config.source_format = "CSV"
            job_config.autodetect = True
            job_config.allow_jagged_rows = True
            job_config.ignore_unknown_values = True
            job_config.skip_leading_rows = 1
            job_config.allow_quoted_newlines = True

            path = f"{dag.params['sg_spend_data_blobs']}/{context['data_interval_end'].strftime('%Y')}/{context['data_interval_end'].strftime('%m')}"
            uri = f"gs://{dag.params['sg_data_bucket']}/{path}/*.csv.gz"

            load_job = bq_client.load_table_from_uri(
                uri, table_id, job_config=job_config
            )
            load_job.result()
            destination_table = bq_client.get_table(table_id)
            logging.info(f"Loaded {destination_table.num_rows} rows to {data}")

        load_data_operator = PythonOperator(
            task_id=f"load_sg_{data}_staging",
            python_callable=bq_load_sg_spend_job,
            op_kwargs={"dag": dag, "data": data},
            dag=dag,
        )

        return load_data_operator

    load_spend_patterns = load_data(data="spend_patterns_raw")
    # load_transaction_panel_summary = load_data(data="transaction_panel_summary")

    convert_placekey_to_olvin_id = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="convert_placekey_to_olvin_id",
        configuration={
            "query": {
                "query": "{% include './bigquery/convert_placekey_to_olvin_id.sql' %}",
                "useLegacySql": "False",
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_IF_NEEDED",
                "destinationTable": {
                    "projectId": "{{ params['project'] }}",
                    "datasetId": "{{ params['staging_data_dataset'] }}",
                    "tableId": "{{ params['spend_patterns_raw_table'] }}",
                },
            },
            "labels": {
                "pipeline": "{{ dag.dag_id }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )

    def append_data(
        data: str,
        queries=query_dict,
        tables=table_dict,
        partitioning=None,
        clustering=None,
        dag=dag,
    ):
        append_sg_spend = BigQueryInsertJobOperator(
            task_id=f"append_sg_{data}",
            configuration={
                "query": {
                    "query": queries[data],
                    "useLegacySql": "False",
                    "writeDisposition": "WRITE_APPEND",
                    "createDisposition": "CREATE_IF_NEEDED",
                    "destinationTable": {
                        "projectId": "{{ params['project'] }}",
                        "datasetId": "{{ params['staging_data_dataset'] }}",
                        "tableId": f"{tables[data]}",
                    },
                    "timePartitioning": partitioning,
                    "clustering": clustering,
                },
                "labels": {
                    "pipeline": "{{ dag.dag_id }}",
                    "task_id": "{{ task.task_id.lower()[:63] }}",
                },
            },
            dag=dag,
            location="EU",
        )
        return append_sg_spend

    # append_transaction_panel_summary = append_data(data="transaction_panel_summary")
    append_spend_patterns = append_data(
        data="spend_patterns",
        partitioning={"field": "spend_date_range_start", "type": "DAY"},
        clustering={"fields": ["fk_sgplaces"]},
    )

    def drop_data(data: str, dag=dag):
        drop_sg_spend = BigQueryDeleteTableOperator(
            task_id=f"drop_sg_{data}_staging",
            deletion_dataset_table=f"{Variable.get('storage_project_id')}.{dag.params['staging_data_dataset']}.{data}",
            dag=dag,
            location="EU",
        )

        return drop_sg_spend

    drop_spend_patterns = drop_data(data="spend_patterns_raw")
    # drop_transaction_panel_summary = drop_data(data="transaction_panel_summary")

    spend_data_end = DummyOperator(task_id="spend_data_tasks_end", dag=dag)

    # spend_patterns
    (
        start
        >> wait_for_spend_patterns
        >> load_spend_patterns
        >> convert_placekey_to_olvin_id
        >> append_spend_patterns
        >> drop_spend_patterns
        >> spend_data_end
    )

    # # transaction_panel_summary
    # (
    #     start
    #     >> wait_for_transaction_panel_summary
    #     >> load_transaction_panel_summary
    #     >> append_transaction_panel_summary
    #     >> drop_transaction_panel_summary
    #     >> spend_data_end
    # )

    return spend_data_end
