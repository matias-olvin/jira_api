"""
DAG ID: dynamic_places
"""
from datetime import datetime

from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.kubernetes_engine import (
    GKEStartPodOperator,
)
from kubernetes.client import models as k8s_models

PY_SCRIPT = "reviews/get_response.py"
API_KEY = "{{ params['api_key'] }}"
VERSION = "{{ var.value.data_version }}"
GCS_BUCKET = "{{ params['places_api_goog_data_bucket'] }}"
GCS_FOLDER = "{{ params['places_api_goog_reviews_folder'] }}"
PROJECT_ID = "{{ params['project'] }}"
PLACES_DATASET = "{{ params['sg_places_dataset'] }}"
PLACES_TABLE = "{{ params['sg_places_table'] }}"
VISITS_ESTIMATION_DATASET = "{{ params['visits_estimation_dataset'] }}"
SNS_POIS_TABLE = "{{ params['sns_pois_table'] }}"
MAX_WORKERS = 20


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

    places_api_goog_reponse = GKEStartPodOperator(
        task_id="places_api_goog",
        name="places_api_goog",
        project_id="{{ params['project'] }}",
        location="europe-west1",
        cluster_name=Variable.get("gke_cluster_name"),
        namespace="gcp-access",
        service_account_name="gcp-access-sa",
        image="{{ params['reviews_image'] }}",
        image_pull_policy="Always",
        get_logs=True,
        is_delete_operator_pod=True,
        container_resources=k8s_models.V1ResourceRequirements(
            requests={"cpu": "500m", "memory": "3.5Gi"},
            limits={"cpu": "1000m", "memory": "4G"},
        ),
        cmds=[
            "python",
            PY_SCRIPT,
            f"--api_key={API_KEY}",
            f"--version={VERSION}",
            f"--gcs_bucket={GCS_BUCKET}",
            f"--gcs_folder={GCS_FOLDER}",
            f"--project_id={PROJECT_ID}",
            f"--places_dataset={PLACES_DATASET}",
            f"--places_table={PLACES_TABLE}",
            f"--visits_estimation_dataset={VISITS_ESTIMATION_DATASET}",
            f"--sns_pois_table={SNS_POIS_TABLE}",
            f"--max_workers={MAX_WORKERS}",
        ],
        labels={
            "pipeline": "{{ dag.dag_id }}",
            "task_id": "{{ task.task_id.lower()[:63] }}",
        },
        dag=dag,
    )

    query_google_reviews = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="query_google_reviews",
        project_id=dag.params["project"],
        configuration={
            "query": {
                "query": "{% include './bigquery/query_google_reviews.sql' %}",
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

    (start >> places_api_goog_reponse >> query_google_reviews)
    return query_google_reviews
