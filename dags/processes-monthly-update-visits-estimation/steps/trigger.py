from datetime import datetime

from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


def trigger(dag, step):
    task = BashOperator(
        task_id=f"trigger_gtvm_metrics_{step}",
        bash_command="gcloud composer environments run {{ params['sns_composer'] }} "
        "--project {{ params['sns_project'] }} "
        "--location {{ params['sns_composer_location'] }} "
        "--impersonate-service-account {{ params['cross_project_service_account'] }} "
        f'dags trigger -- {dag.params["gtvm_metrics_pipeline"]} -c \'{{"step": "{step}"}}\' -e "{{{{ ds }}}} {datetime.now().time()}" '
        "|| true ",  # this line is needed due to gcloud bug.
    )
    return task


def migrate_to_accessible_by_sns(source_dataset, source_table, destination_table, dag):
    task = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id=f"migrate_{destination_table}_to_accessible_by_sns",
        configuration={
            "query": {
                "query": "CREATE OR REPLACE TABLE "
                f"  `{Variable.get('env_project')}.{dag.params['accessible_by_sns_dataset']}.{destination_table}` "
                "COPY "
                f"  `{Variable.get('env_project')}.{source_dataset}.{source_table}`;",
                "useLegacySql": "false",
            },
        },
    )
    return task


def trigger_sns_gtvm_metrics(
    start_task, dag, step, source_dataset, source_table, destination_table
):
    migrate_to_accessible_by_sns_end = migrate_to_accessible_by_sns(
        source_dataset=source_dataset,
        source_table=source_table,
        destination_table=destination_table,
        dag=dag,
    )
    start_task >> migrate_to_accessible_by_sns_end

    trigger_end = trigger(dag=dag, step=step)
    migrate_to_accessible_by_sns_end >> trigger_end

    return trigger_end
