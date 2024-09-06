from datetime import datetime

from airflow.models import Variable, DAG, TaskInstance
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from common.utils.callbacks import check_dashboard_slack_alert
from common.operators.bigquery import SNSBigQueryOperator
from common.operators.exceptions import MarkSuccessOperator
from common.processes.validations import visits_estimation_validation


def register(dag: DAG, start: TaskInstance, env: str):
    """Register tasks on the dag.

    Args:
        dag (airflow.models.DAG): DAG instance to register tasks on.
        start (airflow.models.TaskInstance): Task instance all tasks will
            registered downstream from.

    Returns:
        airflow.models.TaskInstance: The last task node in this section.

    """

    step = "geospatial"
    env_suffix = "" if env == "prod" else "_dev"

    query_aggregate_visits_poi = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="query_aggregate_visits_poi",
        project_id=Variable.get("env_project"),
        configuration={
            "query": {
                "query": "{% include './bigquery/geospatial/observed_visits.sql' %}",
                "useLegacySql": "False",
                "create_session": "True",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id |  replace('.','-') }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )

    delete_aggregate_visits_poi_monitor = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="delete_aggregate_visits_poi_monitor",
        configuration={
            "query": {
                "query": "DELETE `{{ var.value.env_project }}.{{ params['metrics_dataset'] }}.{{ params['visits_aggregated_table_monitor'] }}` where run_date = '{{ ds }}'",
                "useLegacySql": "False",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id |  replace('.','-') }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )

    query_aggregate_visits_poi_monitor = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="query_aggregate_visits_poi_monitor",
        configuration={
            "query": {
                "query": (
                    '{% with visits_aggregated_table="'
                    f"{dag.params['visits_aggregated_table']}"
                    '", visits_aggregated_dataset="'
                    f"{dag.params['visits_estimation_dataset']}"
                    '"%}{% include "./bigquery/geospatial/metrics/observed_visits.sql" %}{% endwith %}'
                ),
                "useLegacySql": "False",
                "destinationTable": {
                    "projectId": "{{ var.value.env_project }}",
                    "datasetId": "{{ params['metrics_dataset'] }}",
                    "tableId": "{{ params['visits_aggregated_table_monitor'] }}",
                },
                "writeDisposition": "WRITE_APPEND",
                "createDisposition": "CREATE_IF_NEEDED",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id |  replace('.','-') }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )

    query_max_historical_date = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="query_max_historical_date",
        project_id=Variable.get("env_project"),
        configuration={
            "query": {
                "query": "{% include './bigquery/geospatial/max_historical_date.sql' %}",
                "useLegacySql": "False",
                "create_session": "True",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id |  replace('.','-') }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )

    copy_max_historical_date_to_accessible_by_sns = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="copy_max_historical_date_to_accessible_by_sns",
        configuration={
            "query": {
                "query": f"CREATE OR REPLACE TABLE `{{{{ var.value.env_project }}}}.{{{{ params['accessible_by_sns_dataset'] }}}}.{step}_{{{{ params['max_historical_date_table'] }}}}` COPY `{{{{ var.value.env_project }}}}.{{{{ params['visits_estimation_dataset'] }}}}.{{{{ params['max_historical_date_table'] }}}}`",
                "useLegacySql": "False",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id |  replace('.','-') }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )

    copy_max_historical_date_into_sns = SNSBigQueryOperator(
        task_id="copy_max_historical_date_into_sns",
        query=f"CREATE OR REPLACE TABLE `{{{{ var.value.sns_project }}}}.{{{{ params['visits_estimation_dataset'] }}}}_{step}{env_suffix}.{{{{ params['max_historical_date_table'] }}}}` COPY `{{{{ var.value.env_project }}}}.{{{{ params['accessible_by_sns_dataset'] }}}}.{step}_{{{{ params['max_historical_date_table'] }}}}`",
    )

    (
        start
        >> query_aggregate_visits_poi
        >> delete_aggregate_visits_poi_monitor
        >> query_aggregate_visits_poi_monitor
        >> query_max_historical_date
        >> copy_max_historical_date_to_accessible_by_sns
        >> copy_max_historical_date_into_sns
    )

    query_model_input_olvin = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="query_model_input_olvin",
        project_id=Variable.get("env_project"),
        configuration={
            "query": {
                "query": "{% include './bigquery/geospatial/model_input_olvin.sql' %}",
                "useLegacySql": "False",
                "create_session": "True",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id |  replace('.','-') }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )

    delete_model_input_olvin_monitor = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="delete_model_input_olvin_monitor",
        configuration={
            "query": {
                "query": "DELETE `{{ var.value.env_project }}.{{ params['metrics_dataset'] }}.{{ params['model_input_olvin_monitor'] }}` where run_date = '{{ ds }}'",
                "useLegacySql": "False",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id |  replace('.','-') }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )

    query_model_input_olvin_monitor = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="query_model_input_olvin_monitor",
        configuration={
            "query": {
                "query": "{% include './bigquery/geospatial/metrics/model_input_olvin.sql' %}",
                "useLegacySql": "False",
                "destinationTable": {
                    "projectId": "{{ var.value.env_project }}",
                    "datasetId": "{{ params['metrics_dataset'] }}",
                    "tableId": "{{ params['model_input_olvin_monitor'] }}",
                },
                "writeDisposition": "WRITE_APPEND",
                "createDisposition": "CREATE_IF_NEEDED",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id |  replace('.','-') }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )

    copy_model_input_olvin_to_accessible_by_sns = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="copy_model_input_olvin_to_accessible_by_sns",
        configuration={
            "query": {
                "query": f"CREATE OR REPLACE TABLE `{{{{ var.value.env_project }}}}.{{{{ params['accessible_by_sns_dataset'] }}}}.{step}_{{{{ params['model_input_olvin_table'] }}}}` COPY `{{{{ var.value.env_project }}}}.{{{{ params['visits_estimation_dataset'] }}}}.{{{{ params['model_input_olvin_table'] }}}}`",
                "useLegacySql": "False",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id |  replace('.','-') }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )

    copy_model_input_olvin_into_sns = SNSBigQueryOperator(
        task_id="copy_model_input_olvin_into_sns",
        query=f"CREATE OR REPLACE TABLE `{{{{ var.value.sns_project }}}}.{{{{ params['visits_estimation_dataset'] }}}}_{step}{env_suffix}.{{{{ params['model_input_olvin_table'] }}}}` COPY `{{{{ var.value.env_project }}}}.{{{{ params['accessible_by_sns_dataset'] }}}}.{step}_{{{{ params['model_input_olvin_table'] }}}}`",
    )

    mark_success_model_input_olvin = MarkSuccessOperator(
        task_id="mark_success_model_input_olvin",
        on_failure_callback=check_dashboard_slack_alert(
            "U03BANPLXJR",
            data_studio_link="https://lookerstudio.google.com/reporting/07196097-8f5b-41d6-a6bf-b1b261bfe89d/page/p_fafydej54c",
            message="Check time series are consistent. Also check regressors.",
        ),
        dag=dag,
    )

    (
        copy_max_historical_date_into_sns
        >> query_model_input_olvin
        >> delete_model_input_olvin_monitor
        >> query_model_input_olvin_monitor
        >> mark_success_model_input_olvin
        >> copy_model_input_olvin_to_accessible_by_sns
        >> copy_model_input_olvin_into_sns
    )

    query_list_sns_pois = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="query_list_sns_pois",
        configuration={
            "query": {
                "query": "{% include './bigquery/general/sns_list.sql' %}",
                "useLegacySql": "False",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id |  replace('.','-') }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )

    query_model_input_grouping_id = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="query_model_input_grouping_id",
        # project_id=Variable.get("compute_project_id"),
        configuration={
            "query": {
                "query": "{% include './bigquery/geospatial/grouping_id.sql' %}",
                "useLegacySql": "False",
                "createSession": "True",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id | replace('.','-') }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )

    query_model_input_grouping_id_gt = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="query_model_input_grouping_id_gt",
        # project_id=Variable.get("compute_project_id"),
        configuration={
            "query": {
                "query": "{% include './bigquery/geospatial/grouping_id_gt.sql' %}",
                "useLegacySql": "False",
                "createSession": "True",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id | replace('.','-') }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )

    query_model_input_events_poi = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="query_model_input_events_poi",
        # project_id=Variable.get("compute_project_id"),
        configuration={
            "query": {
                "query": "{% include './bigquery/geospatial/events_input_poi.sql' %}",
                "useLegacySql": "False",
                "createSession": "True",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id | replace('.','-') }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )

    query_model_input_events_group = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="query_model_input_events_group",
        # project_id=Variable.get("compute_project_id"),
        configuration={
            "query": {
                "query": "{% include './bigquery/geospatial/events_input_group.sql' %}",
                "useLegacySql": "False",
                "createSession": "True",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id | replace('.','-') }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )

    query_model_input_poi_metadata = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="query_model_input_poi_metadata",
        project_id=Variable.get("env_project"),
        configuration={
            "query": {
                "query": "{% include './bigquery/geospatial/poi_metadata.sql' %}",
                "useLegacySql": "False",
                "create_session": "True",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id |  replace('.','-') }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )

    copy_list_sns_pois_to_accessible_by_sns = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="copy_list_sns_pois_to_accessible_by_sns",
        configuration={
            "query": {
                "query": f"CREATE OR REPLACE TABLE `{{{{ var.value.env_project }}}}.{{{{ params['accessible_by_sns_dataset'] }}}}.{step}_{{{{ params['list_sns_pois_table'] }}}}` COPY `{{{{ var.value.env_project }}}}.{{{{ params['visits_estimation_dataset'] }}}}.{{{{ params['list_sns_pois_table'] }}}}`",
                "useLegacySql": "False",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id |  replace('.','-') }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )

    copy_list_sns_pois_into_sns = SNSBigQueryOperator(
        task_id="copy_list_sns_pois_into_sns",
        query=f"CREATE OR REPLACE TABLE `{{{{ var.value.sns_project }}}}.{{{{ params['visits_estimation_dataset'] }}}}_{step}{env_suffix}.{{{{ params['list_sns_pois_table'] }}}}` COPY `{{{{ var.value.env_project }}}}.{{{{ params['accessible_by_sns_dataset'] }}}}.{step}_{{{{ params['list_sns_pois_table'] }}}}`",
    )

    copy_model_input_grouping_id_gt_to_accessible_by_sns = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="copy_model_input_grouping_id_gt_to_accessible_by_sns",
        configuration={
            "query": {
                "query": f"CREATE OR REPLACE TABLE `{{{{ var.value.env_project }}}}.{{{{ params['accessible_by_sns_dataset'] }}}}.{step}_{{{{ params['grouping_id_gt_table'] }}}}` COPY `{{{{ var.value.env_project }}}}.{{{{ params['visits_estimation_dataset'] }}}}.{{{{ params['grouping_id_gt_table'] }}}}`",
                "useLegacySql": "False",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id |  replace('.','-') }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )

    copy_model_input_grouping_id_gt_into_sns = SNSBigQueryOperator(
        task_id="copy_model_input_grouping_id_gt_into_sns",
        query=f"CREATE OR REPLACE TABLE `{{{{ var.value.sns_project }}}}.{{{{ params['visits_estimation_dataset'] }}}}_{step}{env_suffix}.{{{{ params['grouping_id_gt_table'] }}}}` COPY `{{{{ var.value.env_project }}}}.{{{{ params['accessible_by_sns_dataset'] }}}}.{step}_{{{{ params['grouping_id_gt_table'] }}}}`",
    )

    copy_model_input_events_group_to_accessible_by_sns = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="copy_model_input_events_group_to_accessible_by_sns",
        configuration={
            "query": {
                "query": f"CREATE OR REPLACE TABLE `{{{{ var.value.env_project }}}}.{{{{ params['accessible_by_sns_dataset'] }}}}.{step}_{{{{ params['events_holidays_group_table'] }}}}` COPY `{{{{ var.value.env_project }}}}.{{{{ params['visits_estimation_dataset'] }}}}.{{{{ params['events_holidays_group_table'] }}}}`",
                "useLegacySql": "False",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id |  replace('.','-') }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )

    copy_model_input_events_group_into_sns = SNSBigQueryOperator(
        task_id="copy_model_input_events_group_into_sns",
        query=f"CREATE OR REPLACE TABLE `{{{{ var.value.sns_project }}}}.{{{{ params['visits_estimation_dataset'] }}}}_{step}{env_suffix}.{{{{ params['events_holidays_group_table'] }}}}` COPY `{{{{ var.value.env_project }}}}.{{{{ params['accessible_by_sns_dataset'] }}}}.{step}_{{{{ params['events_holidays_group_table'] }}}}`",
    )

    copy_model_input_grouping_id_to_accessible_by_sns = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="copy_model_input_grouping_id_to_accessible_by_sns",
        configuration={
            "query": {
                "query": f"CREATE OR REPLACE TABLE `{{{{ var.value.env_project }}}}.{{{{ params['accessible_by_sns_dataset'] }}}}.{step}_{{{{ params['grouping_id_table'] }}}}` COPY `{{{{ var.value.env_project }}}}.{{{{ params['visits_estimation_dataset'] }}}}.{{{{ params['grouping_id_table'] }}}}`",
                "useLegacySql": "False",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id |  replace('.','-') }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )

    copy_model_input_grouping_id_into_sns = SNSBigQueryOperator(
        task_id="copy_model_input_grouping_id_into_sns",
        query=f"CREATE OR REPLACE TABLE `{{{{ var.value.sns_project }}}}.{{{{ params['visits_estimation_dataset'] }}}}_{step}{env_suffix}.{{{{ params['grouping_id_table'] }}}}` COPY `{{{{ var.value.env_project }}}}.{{{{ params['accessible_by_sns_dataset'] }}}}.{step}_{{{{ params['grouping_id_table'] }}}}`",
    )

    copy_model_input_poi_metadata_to_accessible_by_sns = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="copy_model_input_poi_metadata_to_accessible_by_sns",
        configuration={
            "query": {
                "query": f"CREATE OR REPLACE TABLE `{{{{ var.value.env_project }}}}.{{{{ params['accessible_by_sns_dataset'] }}}}.{step}_{{{{ params['poi_metadata_table'] }}}}` COPY `{{{{ var.value.env_project }}}}.{{{{ params['visits_estimation_dataset'] }}}}.{{{{ params['poi_metadata_table'] }}}}`",
                "useLegacySql": "False",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id |  replace('.','-') }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )

    copy_model_input_poi_metadata_into_sns = SNSBigQueryOperator(
        task_id="copy_model_input_poi_metadata_into_sns",
        query=f"CREATE OR REPLACE TABLE `{{{{ var.value.sns_project }}}}.{{{{ params['visits_estimation_dataset'] }}}}_{step}{env_suffix}.{{{{ params['poi_metadata_table'] }}}}` COPY `{{{{ var.value.env_project }}}}.{{{{ params['accessible_by_sns_dataset'] }}}}.{step}_{{{{ params['poi_metadata_table'] }}}}`",
    )

    copy_model_input_olvin_into_sns >> [
        query_list_sns_pois,
        query_model_input_events_poi,
    ]
    (
        query_list_sns_pois
        >> copy_list_sns_pois_to_accessible_by_sns
        >> copy_list_sns_pois_into_sns
    )

    [
        copy_list_sns_pois_into_sns,
        query_model_input_events_poi,
    ] >> query_model_input_grouping_id
    (
        query_model_input_grouping_id
        >> copy_model_input_grouping_id_to_accessible_by_sns
        >> copy_model_input_grouping_id_into_sns
    )
    copy_model_input_grouping_id_into_sns >> [
        query_model_input_poi_metadata,
        query_model_input_events_group,
        query_model_input_grouping_id_gt,
    ]

    (
        query_model_input_poi_metadata
        >> copy_model_input_poi_metadata_to_accessible_by_sns
        >> copy_model_input_poi_metadata_into_sns
    )
    (
        query_model_input_events_group
        >> copy_model_input_events_group_to_accessible_by_sns
        >> copy_model_input_events_group_into_sns
    )
    (
        query_model_input_grouping_id_gt
        >> copy_model_input_grouping_id_gt_to_accessible_by_sns
        >> copy_model_input_grouping_id_gt_into_sns
    )

    query_model_input = SNSBigQueryOperator(
        task_id="query_model_input",
        query='{% with step="'
        f"{step}"
        '", env="'
        f"{env_suffix}"
        '"%}{% include "./bigquery/geospatial/model_input.sql" %}{% endwith %}',
    )

    [
        copy_model_input_poi_metadata_into_sns,
        copy_model_input_events_group_into_sns,
        copy_model_input_grouping_id_gt_into_sns,
    ] >> query_model_input

    copy_model_input_poi_metadata_into_model_dataset = SNSBigQueryOperator(
        task_id="copy_model_input_poi_metadata_into_model_dataset",
        query=f"""DROP TABLE IF EXISTS `{{{{ var.value.sns_project }}}}.{{{{ params['visits_estimation_model_dataset'] }}}}.{{{{ params['poi_metadata_table'] }}}}`;
        CREATE OR REPLACE TABLE `{{{{ var.value.sns_project }}}}.{{{{ params['visits_estimation_model_dataset'] }}}}.{{{{ params['poi_metadata_table'] }}}}` COPY `{{{{ var.value.sns_project }}}}.{{{{ params['visits_estimation_dataset'] }}}}_{step}{env_suffix}.{{{{ params['poi_metadata_table'] }}}}`;""",
    )

    copy_model_input_into_model_dataset = SNSBigQueryOperator(
        task_id="copy_model_input_into_model_dataset",
        query=f"""DROP TABLE IF EXISTS `{{{{ var.value.sns_project }}}}.{{{{ params['visits_estimation_model_dataset'] }}}}.{{{{ params['model_input_table'] }}}}`;
        CREATE OR REPLACE TABLE `{{{{ var.value.sns_project }}}}.{{{{ params['visits_estimation_model_dataset'] }}}}.{{{{ params['model_input_table'] }}}}` COPY `{{{{ var.value.sns_project }}}}.{{{{ params['visits_estimation_dataset'] }}}}_{step}{env_suffix}.{{{{ params['model_input_table'] }}}}`;""",
    )

    copy_model_input_events_group_into_model_dataset = SNSBigQueryOperator(
        task_id="copy_model_input_events_group_into_model_dataset",
        query=f"""DROP TABLE IF EXISTS `{{{{ var.value.sns_project }}}}.{{{{ params['visits_estimation_model_dataset'] }}}}.{{{{ params['events_holidays_group_table'] }}}}`;
        CREATE OR REPLACE TABLE `{{{{ var.value.sns_project }}}}.{{{{ params['visits_estimation_model_dataset'] }}}}.{{{{ params['events_holidays_group_table'] }}}}` COPY `{{{{ var.value.sns_project }}}}.{{{{ params['visits_estimation_dataset'] }}}}_{step}{env_suffix}.{{{{ params['events_holidays_group_table'] }}}}`;""",
    )

    copy_model_input_max_historical_date_into_model_dataset = SNSBigQueryOperator(
        task_id="copy_model_input_max_historical_date_into_model_dataset",
        query=f"CREATE OR REPLACE TABLE `{{{{ var.value.sns_project }}}}.{{{{ params['visits_estimation_model_dataset'] }}}}.{{{{ params['max_historical_date_table'] }}}}` COPY `{{{{ var.value.sns_project }}}}.{{{{ params['visits_estimation_dataset'] }}}}_{step}{env_suffix}.{{{{ params['max_historical_date_table'] }}}}`",
    )

    create_geospatial_model_output = SNSBigQueryOperator(
        task_id="create_geospatial_model_output",
        query='{% with step="'
        f"{step}"
        '", env="'
        f"{env_suffix}"
        '"%}{% include "./bigquery/geospatial/geospatial_model_output.sql" %}{% endwith %}',
    )

    copy_model_output_to_accessible_by_olvin = SNSBigQueryOperator(
        task_id="copy_model_output_to_accessible_by_olvin",
        query=f"CREATE OR REPLACE TABLE `{{{{ var.value.sns_project }}}}.{{{{ var.value.accessible_by_olvin }}}}.{step}_{{{{ params['model_output_table'] }}}}` COPY `{{{{ var.value.sns_project }}}}.{{{{ params['visits_estimation_dataset'] }}}}_{step}{env_suffix}.{{{{ params['model_output_table'] }}}}`",
    )

    copy_model_output_to_olvin = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="copy_model_output_to_olvin",
        configuration={
            "query": {
                "query": f"CREATE OR REPLACE TABLE `{{{{ var.value.env_project }}}}.{{{{ params['visits_estimation_dataset'] }}}}.{{{{ params['geospatial_output_table'] }}}}` COPY `{{{{ var.value.sns_project }}}}.{{{{ var.value.accessible_by_olvin }}}}.{step}_{{{{ params['model_output_table'] }}}}`",
                "useLegacySql": "False",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id |  replace('.','-') }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )

    validations_start = EmptyOperator(task_id=f"{step}_validation_start")
    copy_model_output_to_olvin >> validations_start

    geospatial_validation = visits_estimation_validation(
        input_table=f"{Variable.get('env_project')}.{dag.params['visits_estimation_dataset']}.{dag.params['geospatial_output_table']}",
        env=env,
        pipeline="visits_estimation",
        step=step,
        run_date="{{ ds }}",
        classes=["fk_sgbrands", "naics_code", "region", "top_category"],
        start_task=validations_start,
    )

    trigger_visualization_pipeline = TriggerDagRunOperator(
        task_id=f"trigger_visualization_pipeline_{step}",
        trigger_dag_id=f"{{{{ dag.dag_id }}}}"+'-visualizations',
        execution_date=f"{{{{ ds }}}} {datetime.now().time()}",
        conf={
            "step": f"{step}",
            "source_table": f"{Variable.get('env_project')}.{dag.params['visits_estimation_dataset']}.{dag.params['geospatial_output_table']}",
        },
        wait_for_completion=True,
        dag=dag,
    )

    mark_success_geospatial_validation = MarkSuccessOperator(
        task_id=f"mark_success_{step}_validation",
        on_failure_callback=check_dashboard_slack_alert(
            "U03BANPLXJR",
            data_studio_link="https://lookerstudio.google.com/reporting/07196097-8f5b-41d6-a6bf-b1b261bfe89d/page/p_j2j42t5i4c",
            message=f"Check metrics on {step}.",
        ),
        dag=dag,
    )

    geospatial_validation >> mark_success_geospatial_validation

    (
        query_model_input
        >> copy_model_input_max_historical_date_into_model_dataset
        >> copy_model_input_poi_metadata_into_model_dataset
        >> copy_model_input_into_model_dataset
        >> copy_model_input_events_group_into_model_dataset
        >> create_geospatial_model_output
        >> copy_model_output_to_accessible_by_olvin
        >> copy_model_output_to_olvin
        >> [validations_start, trigger_visualization_pipeline]
    )
    trigger_visualization_pipeline >> mark_success_geospatial_validation

    return mark_success_geospatial_validation
