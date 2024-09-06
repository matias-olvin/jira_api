"""
DAG ID: dynamic_places
"""

from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
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
    from datetime import datetime

    from common.utils import callbacks

    def get_sg_data_location(dag, curr_prefix):
        """
        Returns: the prefix for path to latest sg data files
        """
        from google.cloud import storage

        gcs_client = storage.Client()
        bucket_name = f"{dag.params['sg_data_bucket']}"
        bucket = gcs_client.bucket(bucket_name)

        for i in range(2, 6):
            blobs = bucket.list_blobs(prefix=curr_prefix)
            subfolder = set()
            for blob in blobs:
                file = blob.name
                subfolder.add(int(file.split("/")[i]))
            if len(str(max(subfolder))) == 1:
                curr_prefix += f"/0{(max(subfolder))}"
            else:
                curr_prefix += f"/{(max(subfolder))}"

        return f"{bucket_name}/{curr_prefix}"

    def bq_load_sg_brands_job(dag, **context):
        """
        Operates similar to BigQueryInsertJobOperator
        With 'load' configuration
        Using bigquery API allows dynanmic sourceUri
        """
        import logging

        from google.cloud import bigquery

        bq_client = bigquery.Client()
        # set table_id to the ID of the table to create.
        table_id = f"{dag.params['storage_project_id']}.{dag.params['staging_data_dataset']}.sg_brands"

        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = "WRITE_TRUNCATE"
        job_config.create_disposition = "CREATE_IF_NEEDED"
        job_config.source_format = "CSV"
        job_config.autodetect = True
        job_config.allow_jagged_rows = True
        job_config.labels = {
            "pipeline": f"{dag.dag_id}",
            "task_id": f"{context['task'].task_id.lower()[:63]}",
        }

        path = get_sg_data_location(
            dag=dag, curr_prefix=dag.params["sg_brands_data_blobs"]
        )
        uri = f"gs://{path}/brand_info*"

        load_job = bq_client.load_table_from_uri(uri, table_id, job_config=job_config)
        load_job.result()
        destination_table = bq_client.get_table(table_id)
        logging.info(f"Loaded {destination_table.num_rows} rows to sg_brands")

    year = f"{datetime.today().year}"
    if len(str(datetime.today().month)) == 1:
        month = f"0{datetime.today().month}"
    else:
        month = f"{datetime.today().month}"

    wait_for_sg_brands = GCSObjectsWithPrefixExistenceSensor(
        task_id="wait_for_sg_brands",
        bucket="{{ params['sg_data_bucket'] }}",
        prefix=f"{{{{ params['sg_brands_data_blobs'] }}}}/{year}/{month}/",
        dag=dag,
        poke_interval=24 * 60 * 60,
        timeout=20 * 24 * 60 * 60,
        soft_fail=False,
        mode="reschedule",
        on_success_callback=callbacks.task_end_custom_slack_alert(
            "U034YDXAD1R",
            "U03BANPLXJR",
            msg_title="monthly brands data found in safegraph-raw-data-olvin-com - pipeline started.",
        ),
    )
    load_sg_brands = PythonOperator(
        task_id="load_sg_brands",
        python_callable=bq_load_sg_brands_job,
        op_kwargs={"dag": dag},
        dag=dag,
    )
    formatting_sg_brands = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="formatting_sg_brands",
        configuration={
            "query": {
                "query": "{% include './bigquery/brands/formatting_sg_brands.sql' %}",
                "useLegacySql": "False",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )
    (start >> wait_for_sg_brands >> load_sg_brands >> formatting_sg_brands)

    return formatting_sg_brands
