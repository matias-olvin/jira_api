from airflow.models import DAG, TaskInstance
from airflow.operators.bash_operator import BashOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
)


def register(
    start: TaskInstance, dag: DAG, mode: str, output_folder: str
) -> TaskInstance:
    """Register tasks on the dag.

    Args:
        start (airflow.models.TaskInstance): Task instance all tasks will
            registered downstream from.
        dag (airflow.models.DAG): DAG instance to register tasks on.

    Returns:
        airflow.models.TaskInstance: The last task node in this section.
        :param output_folder:
        :param mode:
    """
    DATAPROC_CLUSTER_NAME = (
        f"{output_folder}-weather-update-cluster-{{{{ ds_nodash }}}}"
    )

    INPUT_DATA_FOLDER = "gs://{{ params['weather_bucket'] }}/input_data"
    OUTPUT_DATA_FOLDER = f"gs://{{{{ params['weather_bucket'] }}}}/{output_folder}"

    if output_folder == "historical":
        start_date = "{{ ds }}"
        end_date = "{{ next_ds }}"
        get_gb = "false"
        save_folder_export = "gs://{{ params['weather_bucket'] }}/output_data"
        pipeline_label_value = "{{ params['historical_pipeline_label_value'] }}"
    elif output_folder == "forecast":
        start_date = "{{ next_ds }}"
        end_date = "{{ next_execution_date.add(days=8).strftime('%Y-%m-%d') }}"
        get_gb = "false"
        save_folder_export = "gs://{{ params['weather_bucket'] }}/output_data_forecast"
        pipeline_label_value = "{{ params['forecast_pipeline_label_value'] }}"

    create_update_cluster = DataprocCreateClusterOperator(
        gcp_conn_id="google_cloud_olvin_default",
        task_id="create_update_cluster",
        project_id="{{ var.value.env_project }}",
        cluster_config={
            "master_config": {
                "num_instances": 1,
                "machine_type_uri": "n1-standard-2",
            },
            "worker_config": {
                "num_instances": 2,
                "machine_type_uri": "n1-standard-4",
            },
            "initialization_actions": [
                {
                    "executable_file": (
                        "gs://goog-dataproc-initialization-actions-"
                        "{{ params['dataproc_region'] }}/python/pip-install.sh"
                    )
                }
            ],
            "gce_cluster_config": {
                "metadata": {
                    "PIP_PACKAGES": "{{ params['weather_update_pip_req'] }}",
                },
                # "service_account": f"composer@{os.environ['GCP_PROJECT']}.iam.gserviceaccount.com",
            },
            "lifecycle_config": {
                "auto_delete_ttl": {"seconds": 60 * 90},
                "idle_delete_ttl": {"seconds": 60 * 30},
            },
            "software_config": {
                "image_version": "2.0-ubuntu18",
                "properties": {
                    "spark:spark.sql.shuffle.partitions": "128",
                    "spark:spark.memory.fraction": "0.8",
                    "spark:spark.sql.adaptive.enabled": "true",
                },
            },
        },
        region="{{ params['dataproc_region'] }}",
        cluster_name=DATAPROC_CLUSTER_NAME,
        labels={"pipeline": pipeline_label_value},
        dag=dag,
        depends_on_past=True,
        wait_for_downstream=True,
    )
    start >> create_update_cluster

    submit_update_job = DataprocSubmitJobOperator(
        gcp_conn_id="google_cloud_olvin_default",
        task_id="submit_update_job",
        depends_on_past=True,
        wait_for_downstream=False,
        retries=0,
        job={
            "reference": {"project_id": "{{ var.value.env_project }}"},
            "placement": {"cluster_name": DATAPROC_CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": "{{ params['weather_update_main_python_file_uri'] }}",
                "python_file_uris": dag.params["weather_update_python_file_uris"],
                "args": [
                    f"--save_folder={OUTPUT_DATA_FOLDER}",
                    f"--start_date={start_date}",
                    f"--end_date={end_date}",
                    f"--grid_folder_US={INPUT_DATA_FOLDER}/grid_folder/",
                    f"--grid_folder_GB={INPUT_DATA_FOLDER}/grid_folder_gb/",
                    f"--grid_folder_US_extended={INPUT_DATA_FOLDER}/grid_folder_us_extended/",
                    f"--inference_folder={INPUT_DATA_FOLDER}/inference_folder/",
                    f"--neighbourhoods_folder={INPUT_DATA_FOLDER}/neighbourhoods_folder/",
                    f"--csds_folder={INPUT_DATA_FOLDER}/csds_folder/",
                    f"--save_mode={mode}",
                    "--get_US=true",
                    f"--get_GB={get_gb}",
                    "--get_US_extended=true",
                    f"--save_folder_export={save_folder_export}",
                ],
            },
            "labels": {"pipeline": pipeline_label_value},
        },
        region="{{ params['dataproc_region'] }}",
        project_id="{{ var.value.env_project }}",
        dag=dag,
    )
    create_update_cluster >> submit_update_job

    delete_weather_update_cluster = BashOperator(
        task_id="delete_weather_update_cluster",
        depends_on_past=False,
        bash_command=(
            "gcloud dataproc clusters delete "
            f"{DATAPROC_CLUSTER_NAME} "
            "--project={{ var.value.env_project }} "
            "--region={{ params['dataproc_region'] }}"
        ),
        dag=dag,
        trigger_rule="all_done",
        on_failure_callback=None,
        on_retry_callback=None,
        wait_for_downstream=False,
        retries=0,
    )
    submit_update_job >> delete_weather_update_cluster

    return submit_update_job
