"""
DAG ID: demographics_pipeline
"""

from airflow.operators.bash_operator import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import TaskInstance, DAG, Variable
from common.operators.exceptions import MarkSuccessOperator
from common.operators.bigquery import OlvinBigQueryOperator
from common.utils.callbacks import task_end_custom_slack_alert


def register(start: TaskInstance, dag: DAG) -> TaskGroup:
    """Register tasks on the dag.

    Args:
        dag (airflow.models.DAG): DAG instance to register tasks on.
        start (airflow.models.TaskInstance): Task instance all tasks will
            registered downstream from.

    Returns:
        airflow.models.TaskInstance: The last task node in this section.
    """

    with TaskGroup(group_id="trade_area_processing") as group:

    
        dataproc_create = BashOperator(
                task_id="dataproc-create",
                bash_command="{% include './include/bash/dataproc-create.sh' %}",
                env={
                    "INSTANCE": "trade-areas",
                    "USERNAME": "olvin-com",
                    "REPO": "trade-areas",
                    "BRANCH": "main",
                    "GCP_AUTH_SECRET_NAME": "gh_access_trade_areas",
                    "GCP_AUTH_SECRET_VERSION": "1",
                    "PROJECT": f"{Variable.get('env_project')}",
                    "SECRET_PROJECT": f"{Variable.get('prod_project')}",
                    "REGION": "europe-west1",
                    "MASTER_MACHINE_TYPE": "n2-standard-4",
                    "MASTER_BOOT_DISK_SIZE": "500",
                    "MASTER_NUM_LOCAL_SSDS": "1",
                    "WORKER_NUM": "40",
                    "WORKER_MACHINE_TYPE": "n2-standard-8",
                    "WORKER_BOOT_DISK_SIZE": "500",
                    "WORKER_NUM_LOCAL_SSDS": "1",
                    "IMAGE_VERSION": "2.1-debian11",
                    "MAX_IDLE": "30m",  # Change to 30m in prod
                    "MAX_AGE": "10000m",  # Change to time it takes to run +3 hours in prod
                },
            )
        
        dataproc_run_test = BashOperator(
            task_id="dataproc-run-test",
            bash_command="{% include './include/bash/dataproc-run.sh' %}",
            env={
                "INSTANCE": "trade-areas",
                "PROJECT": f"{Variable.get('env_project')}",
                "REGION": "europe-west1",
                "PY_MAIN_FILE": f"gs://{{{{ params['trade_area_bucket'] }}}}/{{{{ params['py_main_file'] }}}}",
                "PY_DIST": f"gs://{{{{ params['trade_area_bucket'] }}}}/{{{{ params['py_dist'] }}}}",
                "GCS_OUTPUT_PATH": f"gs://{{{{ params['trade_area_bucket'] }}}}/{{{{ ds_nodash.format('%Y%m01') }}}}/test/output",
                "GCS_INPUT_PATH": f"gs://{{{{ params['trade_area_bucket'] }}}}/{{{{ ds_nodash.format('%Y%m01') }}}}/test/input",
                "INPUT_SCHEMA_BOOL": "true",
                "OUTPUT_COMPRESSION_BOOL": "true",
                "OUTPUT_COMPRESSION": "zstd",
                "APPEND_MODE": "overwrite",
            },
        )

        query_load_output_test = OlvinBigQueryOperator(
        task_id="query_load_output_test",
        query="{% include './include/bigquery/output/load_test.sql' %}",
        dag=dag,
        )

        check_output = MarkSuccessOperator(
            task_id=f"mark_success_trade_areas_test",
            on_failure_callback=task_end_custom_slack_alert(
            "U05FN3F961X", msg_title=f"Check trade_areas.processed_geometries_test table in BQ."
        ),)

        dataproc_run = BashOperator(
            task_id="dataproc-run",
            bash_command="{% include './include/bash/dataproc-run.sh' %}",
            env={
                "INSTANCE": "trade-areas",
                "PROJECT": f"{Variable.get('env_project')}",
                "REGION": "europe-west1",
                "PY_MAIN_FILE": f"gs://{{{{ params['trade_area_bucket'] }}}}/{{{{ params['py_main_file'] }}}}",
                "PY_DIST": f"gs://{{{{ params['trade_area_bucket'] }}}}/{{{{ params['py_dist'] }}}}",
                "GCS_OUTPUT_PATH": f"gs://{{{{ params['trade_area_bucket'] }}}}/{{{{ ds_nodash.format('%Y%m01') }}}}/output",
                "GCS_INPUT_PATH": f"gs://{{{{ params['trade_area_bucket'] }}}}/{{{{ ds_nodash.format('%Y%m01') }}}}/input",
                "INPUT_SCHEMA_BOOL": "true",
                "OUTPUT_COMPRESSION_BOOL": "true",
                "OUTPUT_COMPRESSION": "zstd",
                "APPEND_MODE": "overwrite",
            },
        )

        start >> dataproc_create >> dataproc_run_test >> query_load_output_test >> check_output >> dataproc_run
    return group
