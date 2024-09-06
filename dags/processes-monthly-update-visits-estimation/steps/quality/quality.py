"""
DAG ID: visits_pipeline
"""
from datetime import datetime

from airflow.models import Variable
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryValueCheckOperator,
)
from common.utils.callbacks import check_dashboard_slack_alert
from common.operators.bigquery import OlvinBigQueryOperator
from common.operators.exceptions import MarkSuccessOperator
from common.processes.validations import visits_estimation_validation


def register(dag, start, env):
    """Register tasks on the dag.

    Args:
        dag (airflow.models.DAG): DAG instance to register tasks on.
        start (airflow.models.TaskInstance): Task instance all tasks will
            registered downstream from.

    Returns:
        airflow.models.TaskInstance: The last task node in this section.


    This function creates (for pois and zipcodes):
        - Quality Check: Based on a few rules we discard some elements and we save them on a table
        - Activity Quality Check: Using as input the activity_table calculated
            at the beginning of the pipeline we inacivate (activity_level = 17):
            - elements which were not run in the inference (code errors, opt not achieved...)
            - elements which the quality check considered as faulty
          the output is appended together with the rest of historical activities

    """
    step = "quality"

    query_quality_check = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="query_quality_check",
        configuration={
            "query": {
                "query": "{% include './bigquery/quality/quality_check.sql' %}",
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

    query_quality_check_monitoring_check = BigQueryValueCheckOperator(
gcp_conn_id="google_cloud_olvin_default",
        task_id="query_quality_check_monitoring_check",
        retries=1,
        sql="SELECT terminate FROM `{{ var.value.env_project }}.{{params['visits_estimation_dataset']}}.{{params['monitoring_quality_check_poi_table']}}` WHERE month_update = FORMAT_DATE('%B', DATE_TRUNC(DATE_SUB( DATE('{{ds}}'), INTERVAL 0 MONTH), MONTH)  )",
        use_legacy_sql=False,
        pass_value=False,
        location="EU",
        dag=dag,
    )

    query_quality_check_monitoring = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="query_quality_check_monitoring",
        configuration={
            "query": {
                "query": "{% include './bigquery/quality/tests/quality_check.sql' %}",
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

    query_quality_output = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="query_quality_output",
        configuration={
            "query": {
                "query": "{% include './bigquery/quality/quality_output.sql' %}",
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
    create_ts_brands_quality = OlvinBigQueryOperator(
        task_id="create_ts_brands_quality",
        query="{% include './bigquery/metrics/brand_agg/quality.sql' %}",
        billing_tier="med",
    )
    create_quality_num_lost_recover = OlvinBigQueryOperator(
        task_id="create_quality_num_lost_recover",
        query="{% include './bigquery/metrics/brand_agg/quality_lost_and_recovered_pois.sql' %}",
        billing_tier="med",
    )

    mark_success_quality = MarkSuccessOperator(task_id="mark_success_quality", dag=dag)

    (
        start
        >> query_quality_check
        >> query_quality_check_monitoring
        >> create_quality_num_lost_recover
        >> query_quality_check_monitoring_check
        >> query_quality_output
        >> create_ts_brands_quality
        >> mark_success_quality
    )

    validations_start = DummyOperator(task_id="quality_validation_start")
    mark_success_quality >> validations_start

    quality_validation = visits_estimation_validation(
        input_table=f"{Variable.get('env_project')}.{dag.params['visits_estimation_dataset']}.{dag.params['quality_output_table']}",
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
            "source_table": f"{Variable.get('env_project')}.{dag.params['visits_estimation_dataset']}.{dag.params['quality_output_table']}",
        },
        wait_for_completion=True,
        dag=dag,
    )

    mark_success_quality_validation = MarkSuccessOperator(
        task_id=f"mark_success_{step}_validation",
        on_failure_callback=check_dashboard_slack_alert(
            "U03BANPLXJR",
            data_studio_link="https://lookerstudio.google.com/reporting/07196097-8f5b-41d6-a6bf-b1b261bfe89d/page/p_j2j42t5i4c",
            message=f"Check metrics on {step}.",
        ),
        dag=dag,
    )
    (
        mark_success_quality
        >> trigger_visualization_pipeline
        >> mark_success_quality_validation
    )

    quality_validation >> mark_success_quality_validation

    query_quality_previous = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="query_quality_previous",
        configuration={
            "query": {
                "query": "{% include './bigquery/quality/quality_previous.sql' %}",
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
    mark_success_post_quality = MarkSuccessOperator(
        task_id="mark_success_post_quality", dag=dag
    )

    (
        mark_success_quality_validation
        >> query_quality_previous
        >> mark_success_post_quality
    )
    return mark_success_post_quality
