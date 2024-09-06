"""
DAG ID: demographics_pipeline
"""
from typing import Dict

from common.operators.bigquery import OlvinBigQueryOperator
from common.operators.exceptions import MarkSuccessOperator
from common.utils import callbacks
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime, timedelta
from airflow.utils.task_group import TaskGroup
from airflow.models import TaskInstance, DAG, Variable
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator


def update_vars(dag,**context):
    Variable.set("bigquery_batch_sent_dataset_name", f"{dag.params['postgres_batch_dataset']}")
    Variable.set("bigquery_batch_sent_table_name", f"{dag.params['sgplacetradearearaw_table']}")
    Variable.set("postgres_batch_table_name", f"{dag.params['sgplacetradearearaw_table']}")
    return

def register(
    start: TaskInstance,
    dag: DAG,
    trigger_dag_id: str,
    poke_interval: int = 60,
    conf: Dict = None,
) -> TaskGroup:

    with TaskGroup(group_id="send_to_postgres") as group:

        query_snapshot_to_postgres = OlvinBigQueryOperator(
        task_id="query_snapshot_to_postgres",
        query="{% include './include/bigquery/output/snapshot_to_postgres.sql' %}",
        billing_tier="high",
        dag=dag,
        )
        start >> query_snapshot_to_postgres

        update_airflow_vars = PythonOperator(
            task_id="update_airflow_vars",
            python_callable=update_vars,
            op_kwargs={"dag": dag},            
            provide_context=True,
            dag=dag,
        )
        sleep_task = BashOperator(
                    task_id=f"sleep_to_update_send_pipeline",
                    bash_command="sleep 240",
                    dag=dag,
        )

        check_trade_areas_tests = MarkSuccessOperator(
            task_id="check_trade_areas_tests",
            on_success_callback=callbacks.task_end_custom_slack_alert(
                "U05FN3F961X", msg_title="Check Trade Area table."
            ),
        )

        def exec_delta_fn(execution_date):
            """
            > It takes the execution date and returns the execution date minus the difference between the
            execution date and the execution date with the time set to 00:00:01

            :param execution_date: The execution date of the task instance
            :return: The execution date minus the difference in seconds between the source and target
            execution dates.
            """
            source_exec_date = execution_date.replace(tzinfo=None)

            target_exec_date = source_exec_date.strftime('%Y-%m-%dT00:00:01')
            target_exec_date = datetime.strptime(f"{target_exec_date}",'%Y-%m-%dT%H:%M:%S')

            diff = source_exec_date - target_exec_date
            diff_seconds = diff.total_seconds()

            return execution_date - timedelta(seconds=diff_seconds)

        if conf is None:
            conf = {}
        
        trigger_dag_run = TriggerDagRunOperator(
            task_id=f"trigger_{trigger_dag_id}",
            trigger_dag_id=f"{{{{ params['{trigger_dag_id}'] }}}}",  # id of the child DAG
            conf=conf,  # Use to pass variables to triggered DAG.
            execution_date="{{ ds }}T00:00:01+00:00",
            reset_dag_run=True,
        )
        dag_run_sensor = ExternalTaskSensor(
            task_id=f"{trigger_dag_id}_sensor",
            external_dag_id=f"{{{{ params['{trigger_dag_id}'] }}}}",  # same as above
            external_task_id="delete_staging_SGPlaceTradeAreaRaw",
            external_task_ids=None,  # Use to wait for specifc tasks.
            external_task_group_id=None,  # Use to wait for specifc task group.
            execution_date_fn=exec_delta_fn,
            check_existence=True,  # Check DAG exists.
            mode="reschedule",
            poke_interval=poke_interval,  # choose appropriate time to poke, if dag runs for 10 minutes, poke every 5 for exmaple
        )

        query_snapshot_to_postgres >> check_trade_areas_tests >> update_airflow_vars >> sleep_task >> trigger_dag_run >> dag_run_sensor
        bucket = f"{dag.params['postgres-final-database-tables-bucket']}"
        database_table = f"{dag.params['sgplacetradearearaw_table']}"

        # Clear monthly data
        clear_job = GCSDeleteObjectsOperator(
            gcp_conn_id="google_cloud_olvin_default",
            task_id=f"clearGCS_{database_table}",
            depends_on_past=False,
            dag=dag,
            bucket_name=bucket,
            prefix=f"{database_table}/{{{{ execution_date.strftime('%Y') }}}}/{{{{ execution_date.strftime('%m') }}}}/",
        )
        # Export to GCS
        export_job = BigQueryInsertJobOperator(
            gcp_conn_id="google_cloud_olvin_default",
            task_id=f"exportToGcs_{database_table}",
            configuration={
                "extract": {
                    "destinationUris": (
                        (
                            f"gs://{bucket}/{database_table}/{{{{ execution_date.strftime('%Y') }}}}/{{{{ execution_date.strftime('%m') }}}}/*.csv.gz"
                        )
                    ),
                    "printHeader": True,
                    "destinationFormat": "CSV",
                    "fieldDelimiter": "\t",
                    "compression": "GZIP",
                    "sourceTable": {
                        "projectId": "{{ var.value.env_project }}",
                        "datasetId": "{{ params['postgres_batch_dataset'] }}",
                        "tableId": "{{ params['sgplacetradearearaw_table'] }}",
                    },
                },
                "labels": {
                    "pipeline": "{{ dag.dag_id |  replace('.','-') }}",
                    "task_id": "{{ task.task_id.lower()[:63] |  replace('.','-') }}",
                },
            },
            dag=dag,
        )
        dag_run_sensor >> clear_job >> export_job
   
    return group