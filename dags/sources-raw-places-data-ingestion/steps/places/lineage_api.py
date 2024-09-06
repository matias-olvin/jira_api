"""
DAG ID: dynamic_places
"""
from datetime import datetime

from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from kubernetes.client import models as k8s_models

from common.operators.kubernetes_engine import GKEStartPodOperatorV2


PY_SCRIPT = "lineage/get_response.py"
API_URL = "{{ params['api_url'] }}"
API_KEY = "{{ params['api_key'] }}"
GCS_BUCKET = "{{ params['sg_data_bucket'] }}"
GCS_FOLDER = "{{ params['sg_lineage_api_response_folder'] }}"
PROJECT_ID = "{{ params['project'] }}"
DATASET_ID = "{{ params['staging_data_dataset'] }}"
TABLE_ID = "{{ params['all_places_table'] }}"
MAX_WORKERS = 50


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

    def bq_load_lineage_job(dag, **context):
        """
        Operates similar to BigQueryInsertJobOperator
        With 'load' configuration
        Using bigquery API allows dynanmic sourceUri
        """
        import logging
        from datetime import datetime

        from google.cloud import bigquery

        bq_client = bigquery.Client()
        # set table_id to the ID of the table to create.
        table_id = f"{dag.params['storage_project_id']}.{dag.params['staging_data_dataset']}.lineage"

        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = "WRITE_TRUNCATE"
        job_config.create_disposition = "CREATE_IF_NEEDED"
        job_config.source_format = "NEWLINE_DELIMITED_JSON"
        job_config.autodetect = True
        job_config.labels = {
            "pipeline": f"{dag.dag_id}",
            "task_id": f"{context['task'].task_id.lower()[:63]}",
        }

        bucket_name = f"{dag.params['sg_data_bucket']}"
        curr_prefix = f"{dag.params['sg_lineage_api_response_folder']}"
        year = datetime.now().year
        if len(str(datetime.now().month)) == 1:
            month = f"0{datetime.now().month}"
        else:
            month = f"{datetime.now().month}"

        uri = f"gs://{bucket_name}/{curr_prefix}/{year}/{month}/batch*"
        load_job = bq_client.load_table_from_uri(uri, table_id, job_config=job_config)
        load_job.result()
        destination_table = bq_client.get_table(table_id)
        logging.info(f"Loaded {destination_table.num_rows} rows to lineage")

    year = f"{datetime.now().strftime('%Y')}"
    month = f"{datetime.now().strftime('%m')}"

    clear_lineage_gcs = GCSDeleteObjectsOperator(
        task_id="clear_lineage_gcs",
        depends_on_past=False,
        dag=dag,
        bucket_name=f"{GCS_BUCKET}",
        prefix=f"{GCS_FOLDER}/{year}/{month}",
    )

    sg_lineage_api_response = GKEStartPodOperatorV2(
        task_id="query_sg_lineage_api",
        name="query_sg_lineage",
        project_id="{{ params['project'] }}",
        location="europe-west1",
        cluster_name=Variable.get("gke_cluster_name"),
        namespace="gcp-access",
        service_account_name="gcp-access-sa",
        image="{{ params['lineage_api_image'] }}",
        image_pull_policy="Always",
        get_logs=True,
        is_delete_operator_pod=True,
        container_resources=k8s_models.V1ResourceRequirements(
            requests={"cpu": "1000m", "memory": "8Gi"}
            # limits={"cpu": "1000m", "memory": "4Gi"},
        ),
        cmds=[
            "python",
            PY_SCRIPT,
            f"--api_url={API_URL}",
            f"--api_key={API_KEY}",
            f"--gcs_bucket={GCS_BUCKET}",
            f"--gcs_folder={GCS_FOLDER}",
            f"--project_id={PROJECT_ID}",
            f"--dataset_id={DATASET_ID}",
            f"--table_id={TABLE_ID}",
            f"--max_workers={MAX_WORKERS}",
        ],
        labels={
            "pipeline": "{{ dag.dag_id }}",
            "task_id": "{{ task.task_id.lower()[:63] }}",
        },
        dag=dag,
    )
    sg_lineage_to_bq = PythonOperator(
        task_id="sg_lineage_to_bq",
        python_callable=bq_load_lineage_job,
        op_kwargs={"dag": dag},
        dag=dag,
    )
    (start >> clear_lineage_gcs >> sg_lineage_api_response >> sg_lineage_to_bq)
    return sg_lineage_to_bq
