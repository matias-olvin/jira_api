"""
DAG ID: real_visits
"""
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
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

    """

    snapshot_gtvm_output = OlvinBigQueryOperator(
        task_id="snapshot_gtvm_output",
        query="{% include './bigquery/adjustments/volume/snapshot_gtvm_output.sql' %}",
    )
    start >> snapshot_gtvm_output

    volume_query = BigQueryInsertJobOperator(
        task_id="volume_query",
        project_id=Variable.get("env_project"),
        configuration={
            "query": {
                "query": "{% include './bigquery/adjustments/volume/factor_gtvm_visits_estimation.sql' %}",
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
    snapshot_gtvm_output >> volume_query


    create_ts_brands_volume = OlvinBigQueryOperator(
        task_id="create_ts_brands_volume",
        query="{% include './bigquery/metrics/brand_agg/volume.sql' %}",
        billing_tier="med",
    )
    volume_query >> create_ts_brands_volume

    mark_success_volume = MarkSuccessOperator(task_id="mark_success_volume", dag=dag)
    create_ts_brands_volume >> mark_success_volume

    step = "adjustments_volume"

    validations_start = DummyOperator(task_id=f"{step}_validation_start")
    mark_success_volume >> validations_start

    volume_volume_validation = visits_estimation_validation(
        input_table=f"{Variable.get('env_project')}.{dag.params['visits_estimation_dataset']}.{dag.params['adjustments_volume_output_table']}",
        env=env,
        pipeline="visits_estimation",
        step=step,
        run_date="{{ ds }}",
        classes=["fk_sgbrands", "naics_code", "region", "top_category"],
        start_task=validations_start,
    )

    mark_success_volume_validation = MarkSuccessOperator(
        task_id=f"mark_success_{step}_validation",
        on_failure_callback=check_dashboard_slack_alert(
            "U03BANPLXJR", message=f"Check metrics on {step}."
        ),
        dag=dag,
    )

    volume_volume_validation >> mark_success_volume_validation

    return mark_success_volume_validation
