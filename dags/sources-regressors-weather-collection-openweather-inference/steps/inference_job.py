from airflow.models import DAG, TaskInstance
from airflow.operators.bash_operator import BashOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
)


def register(start: TaskInstance, dag: DAG) -> TaskInstance:
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
    DATAPROC_CLUSTER_NAME = "weather-inference-cluster-{{ ds_nodash }}"

    DATA_FOLDER = "gs://{{ params['weather_bucket'] }}"

    create_inference_cluster = DataprocCreateClusterOperator(
        gcp_conn_id="google_cloud_olvin_default",
        task_id="create_inference_cluster",
        project_id="{{ var.value.env_project }}",
        cluster_config={
            "master_config": {
                "num_instances": 1,
                "machine_type_uri": "n1-highmem-4",
            },
            "worker_config": {
                "num_instances": 4,
                "machine_type_uri": "e2-highmem-16",
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
                    "PIP_PACKAGES": "{{ params['weather_inference_pip_req'] }}",
                },
                # "service_account": f"composer@{os.environ['GCP_PROJECT']}.iam.gserviceaccount.com",
            },
            "lifecycle_config": {
                "auto_delete_ttl": {"seconds": 3600 * 8},
                "idle_delete_ttl": {"seconds": 600},
            },
            "software_config": {
                "image_version": "2.0-ubuntu18",
                "properties": {
                    # there are around 6700 cells, so this is at least one partition by timeseries
                    "spark:spark.sql.shuffle.partitions": "8192",
                    # The final inference results need to contain a whole timeseries in a partition, this makes it
                    # possible
                    "spark:spark.sql.execution.arrow.maxRecordsPerBatch": "100000000",
                    # All these options make sure behaviour of spark 3 is identical to spark 2
                    "spark:spark.sql.adaptive.enabled": "false",
                    "spark:spark.sql.csv.filterPushdown.enabled": "false",
                    "spark:spark.sql.sources.bucketing.autoBucketedScan.enabled": "false",
                    "spark:spark.sql.optimizer.dynamicPartitionPruning.enabled": "false",
                    "spark:spark.sql.jsonGenerator.ignoreNullFields": "false",
                    "spark:spark.sql.hive.convertInsertingPartitionedTable": "false",
                    "spark:spark.sql.inMemoryColumnarStorage.enableVectorizedReader": "false",
                    "spark:spark.sql.adaptive.coalescePartitions.enabled": "false",
                    "spark:spark.sql.adaptive.localShuffleReader.enabled": "false",
                    "spark:spark.driver.maxResultSize": "0",
                    "spark:spark.sql.adaptive.skewJoin.enabled": "false",
                    "spark:spark.sql.avro.filterPushdown.enabled": "false",
                    "spark:spark.sql.hive.convertMetastoreCtas": "false",
                    "spark:spark.sql.optimizer.enableJsonExpressionOptimization": "false",
                    # Reduce the amount of memory spark keeps for contingencies so more is available for the workers
                    "spark:spark.memory.fraction": "0.8",
                    # This was suggested by Unravel once and seems to work well
                    "spark:spark.sql.autoBroadcastJoinThreshold": "23463715",
                },
            },
            "endpoint_config": {
                "enable_http_port_access": True,
            },
        },
        region="{{ params['dataproc_region'] }}",
        cluster_name=DATAPROC_CLUSTER_NAME,
        labels={
            "pipeline": "{{ dag.dag_id }}",
            "task_id": "{{ task.task_id.lower()[:63] }}",
        },
        dag=dag,
    )
    start >> create_inference_cluster

    submit_inference_job = DataprocSubmitJobOperator(
        gcp_conn_id="google_cloud_olvin_default",
        task_id="submit_inference_job",
        depends_on_past=False,
        job={
            "reference": {"project_id": "{{ var.value.env_project }}"},
            "placement": {"cluster_name": DATAPROC_CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": "{{ params['weather_inference_main_python_file_uri'] }}",
                "python_file_uris": dag.params["weather_inference_python_file_uris"],
                "args": [
                    f"--save_folder={DATA_FOLDER}/input_data",
                    f"--final_weather_folder={DATA_FOLDER}/historical",
                ],
            },
            "labels": {
                "pipeline": "{{ dag.dag_id }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        region="{{ params['dataproc_region'] }}",
        project_id="{{ var.value.env_project }}",
        dag=dag,
    )
    create_inference_cluster >> submit_inference_job

    delete_weather_inference_cluster = BashOperator(
        task_id="delete_weather_inference_cluster",
        depends_on_past=False,
        bash_command=(
            "gcloud dataproc clusters delete "
            f"{DATAPROC_CLUSTER_NAME} "
            "--region={{ params['dataproc_region'] }}"
        ),
        trigger_rule="all_done",
        dag=dag,
    )
    submit_inference_job >> delete_weather_inference_cluster

    return submit_inference_job
