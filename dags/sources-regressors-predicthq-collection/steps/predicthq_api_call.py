"""
DAG ID: predicthq_events_collection
"""
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
# from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)

from common.operators.kubernetes_engine import GKEStartPodOperatorV2

# from predicthq_events.collection.steps.function.daily_api_call import call_api


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

    PY_SCRIPT = "predicthq/daily_api_call.py"
    PROJECT_ID = Variable.get("env_project")
    ACCESS_TOKEN = "TIXcrGAvwCSei5B0aPP9h_SNVl5Ki6rMmSbbRKdy"
    BUCKET = "predicthq-events-data"
    FILE_PATH = "updated_data_20220504/daily_data.csv"

    delay = BashOperator(task_id="delay", dag=dag, bash_command="sleep 10")


    predicthq_api_call = GKEStartPodOperatorV2(
        task_id="predicthq_api_call",
        name="predicthq_api_call",
        project_id="{{ var.value.env_project }}",
        location="europe-west1",
        cluster_name=Variable.get("gke_cluster_name"),
        namespace="gcp-access",
        service_account_name="gcp-access-sa",
        image="{{ params['predicthq_image'] }}",
        image_pull_policy="Always",
        get_logs=True,
        is_delete_operator_pod=True,
        cmds=[
            "python",
            PY_SCRIPT,
            f"--project_id={PROJECT_ID}",
            f"--access_token={ACCESS_TOKEN}",
            f"--destination_bucket={BUCKET}",
            f"--destination_file_path={FILE_PATH}",
        ],
        labels={
            "pipeline": "{{ dag.dag_id }}",
            "task_id": "{{ task.task_id.lower()[:63] }}",
        },
        dag=dag,
    )
    load_daily_data = GCSToBigQueryOperator(
        task_id="load_daily_data",
        bucket="predicthq-events-data",
        source_objects=["updated_data_20220504/daily_data.csv"],
        destination_project_dataset_table=f"{Variable.get('env_project')}.{dag.params['regressors_staging_dataset']}.{dag.params['daily_data_table']}",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        source_format="CSV",
        allow_jagged_rows=True,
        allow_quoted_newlines=True,
        # quote_character= "",
        skip_leading_rows=1,
        dag=dag,
        location="EU",
    )

    (start >> delay >> predicthq_api_call >> load_daily_data)

    return load_daily_data
