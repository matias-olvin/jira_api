from datetime import datetime, timedelta
from typing import Dict, List

from airflow.models import TaskInstance, Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryValueCheckOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup

from common.operators.bigquery import (
    OlvinBigQueryOperator,
    SNSBigQueryOperator,
)
from common.operators.exceptions import MarkSuccessOperator
from common.utils.queries import end_dag_timer_query, start_dag_timer_query


def dag_run_trigger_operator(env: str, dag_id: str) -> BashOperator:
    """
    It triggers a DAG run in a different Composer environment

    Args:
      dag_id (str): str - the dag_id of the dag you want to trigger

    Returns:
      A BashOperator object.
    """
    bash = (
        "gcloud composer environments run {{ params['sns-prod-composer-name'] }} "
        f"--project {Variable.get('sns_project')} "
        "--location {{ params['sns-prod-composer-location'] }} "
        "--impersonate-service-account {{ params['cross-project-composer-worker-service-account'] }} "
        f'dags trigger -- {dag_id} -c \'{{"env": "{env}"}}\' -e "{{{{ ds }}}} {datetime.now().time()}" '
        "|| true "  # this line is needed due to gcloud bug.
    )
    return BashOperator(
        task_id=f"trigger_{dag_id}",
        bash_command=bash,
    )


def wait_for_sns_dag_run_completion(dag_id: str, retries: int = 60, retry_delay: int = 60 * 2) -> BigQueryValueCheckOperator:
    """
    This function will check the status of a dag run in the `sns_dags_status` table in the
    `sns_project` project

    Args:
      env (str): str - the environment you're running the dag in (dev, prod, etc)
      dag_id (str): The dag_id of the dag you want to wait for.

    Returns:
      A BigQueryValueCheckOperator
    """
    accessible_dataset = Variable.get("accessible_by_olvin")
    accessible_table = "{{ params['sns_dags_status_table'] }}"
    query = (
        "SELECT completed "
        "FROM "
        f"   `{Variable.get('sns_project')}.{accessible_dataset}.{accessible_table}` "
        "WHERE "
        f"   dag_id = '{dag_id}' "
        "   AND run_date = DATE('{{ ds }}') "
    )

    return BigQueryValueCheckOperator(
        task_id=f"check_{dag_id}_status",
        sql=query,
        pass_value=True,
        use_legacy_sql=False,
        retries=retries,
        retry_delay=retry_delay,
    )


def trigger_dag_run(
    start: TaskInstance,
    env: str,
    trigger_dag_id: str,
    source_table: List[str] = None,
    destination_table: List[str] = None,
    retries: int = 60,
    retry_delay: int = 60 * 2
) -> TaskInstance:
    """
    This function takes in a start task, an environment, a source table, a destination table, and a
    trigger DAG ID. It then creates a DAG that migrates the source table to SNS, triggers the
    DAG, waits for the DAG to complete, and then migrates the SNS table to the destination table

    Args:
      start (TaskInstance): TaskInstance - the task instance that will trigger the DAG
      env (str): The environment you're running this in.
      source_table (str or List[str]): The table you want to migrate from.
      destination_table (str or List[str]): The table that you want to migrate to.
      trigger_dag_id (str): The id of the dag you want to trigger
      retries (int): The number of times the sensor checks the triggered dag for completion (in seconds)
      retry_delay (int): The time between each sensor check (in seconds)

    Returns:
      The last task in the DAG.
    """
    trigger_sns_dag = dag_run_trigger_operator(env=env, dag_id=trigger_dag_id)
    wait_for_sns_dag = wait_for_sns_dag_run_completion(dag_id=trigger_dag_id, retries=retries, retry_delay=retry_delay)
    env_suffix = "" if env == "prod" else "_dev"

    if source_table is None and destination_table is None:
        start >> trigger_sns_dag >> wait_for_sns_dag
        return wait_for_sns_dag

    # Only acessed if source_table is not None and destination_table is not None
    elif source_table is not None and destination_table is not None:
        start_source_migration = DummyOperator(
            task_id=f"start_source_migration_for_{trigger_dag_id}"
        )
        end_source_migration = DummyOperator(
            task_id=f"end_source_migration_for_{trigger_dag_id}"
        )
        start >> start_source_migration

        for table in source_table:
            project_name, dataset_name, table_name = table.split(".")[0:]
            migrate_1 = OlvinBigQueryOperator(
                gcp_conn_id="google_cloud_olvin_default",
                task_id=f"move_{table_name}_to_sns_for_{trigger_dag_id}",
                query=f"""CREATE OR REPLACE TABLE `{project_name}.accessible_by_sns.{dataset_name}-{table_name}` \
                    COPY `{table}`""",
            )

            migrate_2 = SNSBigQueryOperator(
                task_id=f"move_{table_name}_to_sns_dataset_for_{trigger_dag_id}",
                query=f"""CREATE OR REPLACE TABLE `sns-vendor-olvin-poc.{dataset_name}{env_suffix}.{table_name}` \
                    COPY `{project_name}.accessible_by_sns.{dataset_name}-{table_name}`""",
            )
            start_source_migration >> migrate_1 >> migrate_2 >> end_source_migration

        end_source_migration >> trigger_sns_dag >> wait_for_sns_dag

        start_destination_migration = DummyOperator(
            task_id=f"start_destination_migration_for_{trigger_dag_id}"
        )
        end_destination_migration = DummyOperator(
            task_id=f"end_destination_migration_for_{trigger_dag_id}"
        )
        wait_for_sns_dag >> start_destination_migration

        for table in destination_table:
            project_name, dataset_name, table_name = table.split(".")[0:]
            migrate_1 = SNSBigQueryOperator(
                task_id=f"move_{table_name}_to_olvin_for_{trigger_dag_id}",
                query=f"""CREATE OR REPLACE TABLE `sns-vendor-olvin-poc.accessible_by_olvin{env_suffix}.{dataset_name}-{table_name}` \
                    COPY `sns-vendor-olvin-poc.{dataset_name}{env_suffix}.{table_name}`""",
            )

            migrate_2 = OlvinBigQueryOperator(
                gcp_conn_id="google_cloud_olvin_default",
                task_id=f"move_{table_name}_to_olvin_dataset_for_{trigger_dag_id}",
                query=f"""CREATE OR REPLACE TABLE `{table}` \
                    COPY `sns-vendor-olvin-poc.accessible_by_olvin{env_suffix}.{dataset_name}-{table_name}`""",
            )
            (
                start_destination_migration
                >> migrate_1
                >> migrate_2
                >> end_destination_migration
            )

        return end_destination_migration

    # Only acessed if source_table is not None and destination_table is None
    elif source_table is not None:
        start_source_migration = DummyOperator(
            task_id=f"start_source_migration_for_{trigger_dag_id}"
        )
        end_source_migration = DummyOperator(
            task_id=f"end_source_migration_for_{trigger_dag_id}"
        )
        start >> start_source_migration

        for table in source_table:
            project_name, dataset_name, table_name = table.split(".")[0:]
            migrate_1 = OlvinBigQueryOperator(
                gcp_conn_id="google_cloud_olvin_default",
                task_id=f"move_{table_name}_to_sns_for_{trigger_dag_id}",
                query=f"""CREATE OR REPLACE TABLE `{project_name}.accessible_by_sns.{dataset_name}-{table_name}` \
                    COPY `{table}`""",
            )

            migrate_2 = SNSBigQueryOperator(
                task_id=f"move_{table_name}_to_sns_dataset_for_{trigger_dag_id}",
                query=f"""CREATE OR REPLACE TABLE `sns-vendor-olvin-poc.{dataset_name}{env_suffix}.{table_name}` \
                    COPY `{project_name}.accessible_by_sns.{dataset_name}-{table_name}`""",
            )
            start_source_migration >> migrate_1 >> migrate_2 >> end_source_migration

        end_source_migration >> trigger_sns_dag >> wait_for_sns_dag
        return wait_for_sns_dag

    # Only accessed if source_table is none and destination_table is not none
    else:
        start_destination_migration = DummyOperator(
            task_id=f"start_destination_migration_for_{trigger_dag_id}"
        )
        end_destination_migration = DummyOperator(
            task_id=f"end_destination_migration_for_{trigger_dag_id}"
        )
        start >> trigger_sns_dag >> wait_for_sns_dag >> start_destination_migration

        for table in destination_table:
            project_name, dataset_name, table_name = table.split(".")[0:]
            migrate_1 = SNSBigQueryOperator(
                task_id=f"move_{table_name}_to_olvin_for_{trigger_dag_id}",
                query=f"""CREATE OR REPLACE TABLE `sns-vendor-olvin-poc.accessible_by_olvin{env_suffix}.{dataset_name}-{table_name}` \
                    COPY `sns-vendor-olvin-poc.{dataset_name}{env_suffix}.{table_name}`""",
            )

            migrate_2 = OlvinBigQueryOperator(
                gcp_conn_id="google_cloud_olvin_default",
                task_id=f"move_{table_name}_to_olvin_dataset_for_{trigger_dag_id}",
                query=f"""CREATE OR REPLACE TABLE `{table}` \
                    COPY `sns-vendor-olvin-poc.accessible_by_olvin{env_suffix}.{dataset_name}-{table_name}`""",
            )
            (
                start_destination_migration
                >> migrate_1
                >> migrate_2
                >> end_destination_migration
            )
        return end_destination_migration


def trigger_sensor_group_w_timer(
    start: TaskInstance,
    trigger_dag_id: str,
    poke_interval: int = 60,
    conf: Dict = None,
    mark_success_task: bool = True,
    trigger_rule: str = "all_success",
    external_task_id: str = "end",
) -> TaskGroup:
    """
    It takes the execution date and returns the execution date minus the difference between the
    execution date and the execution date with the time set to 00:00:00

    Args:
        start (TaskInstance): start task instance.
        trigger_dag_id (str): The id of the dag to be triggered, must be key from config file.
        poke_interval (int): The interval at which the sensor will check for the DAG to be done.
        conf (Dict): The configuration to pass to the triggered DAG.
        mark_success_task (bool): Whether to include a mark success task before continuing.
        trigger_rule (str): The trigger rule for the first task in group.
        external_task_id (str): The task id to wait for in the external task sensor.

    Returns:
        TaskGroup: A task group that contains the trigger, sensor, and timer tasks.
    """

    def exec_delta_fn(execution_date):
        source_exec_date = execution_date.replace(tzinfo=None)

        target_exec_date = source_exec_date.strftime("%Y-%m-%dT00:00:00")
        target_exec_date = datetime.strptime(f"{target_exec_date}", "%Y-%m-%dT%H:%M:%S")

        diff = source_exec_date - target_exec_date
        diff_seconds = diff.total_seconds()

        return execution_date - timedelta(seconds=diff_seconds)

    with TaskGroup(group_id=f"trigger_{trigger_dag_id}_task_group") as group:
        if conf is None:
            conf = {}

        param_trigger_dag_id = f"{{{{ params['{trigger_dag_id}'] }}}}"

        start_timer_query = start_dag_timer_query(dag_id=param_trigger_dag_id)

        end_timer_query = end_dag_timer_query(dag_id=param_trigger_dag_id)

        start_timer_task = OlvinBigQueryOperator(
            task_id=f"start_timer_for_{trigger_dag_id}",
            query=start_timer_query,
            trigger_rule=trigger_rule,
        )

        trigger_dag_run = TriggerDagRunOperator(
            task_id=f"trigger_{trigger_dag_id}",
            trigger_dag_id=param_trigger_dag_id,  # id of the child DAG
            conf=conf,  # Use to pass variables to triggered DAG.
            execution_date="{{ ds }}",
            reset_dag_run=True,
        )
        dag_run_sensor = ExternalTaskSensor(
            task_id=f"{trigger_dag_id}_sensor",
            external_dag_id=param_trigger_dag_id,  # same as above
            external_task_id=external_task_id,
            external_task_ids=None,  # Use to wait for specifc tasks.
            external_task_group_id=None,  # Use to wait for specifc task group.
            execution_date_fn=exec_delta_fn,
            check_existence=True,  # Check DAG exists.
            mode="reschedule",
            poke_interval=poke_interval,  # choose appropriate time to poke, if dag runs for 10 minutes, poke every 5 for exmaple
        )

        end_timer_task = OlvinBigQueryOperator(
            task_id=f"end_timer_for_{trigger_dag_id}",
            query=end_timer_query,
        )

        if mark_success_task:
            task_to_fail = MarkSuccessOperator(task_id=f"mark_success_{trigger_dag_id}")

            (
                start
                >> start_timer_task
                >> trigger_dag_run
                >> dag_run_sensor
                >> task_to_fail
                >> end_timer_task
            )
        else:
            (
                start
                >> start_timer_task
                >> trigger_dag_run
                >> dag_run_sensor
                >> end_timer_task
            )

    return group
