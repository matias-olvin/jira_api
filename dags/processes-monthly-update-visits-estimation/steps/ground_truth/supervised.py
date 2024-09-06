from datetime import datetime

from airflow.models import DAG, TaskInstance, Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.utils.task_group import TaskGroup
from common.operators.bigquery import OlvinBigQueryOperator
from common.operators.exceptions import MarkSuccessOperator
from common.processes.triggers import trigger_dag_run
from common.processes.validations import visits_estimation_validation
from common.utils.callbacks import check_dashboard_slack_alert


def register(dag: DAG, start: TaskInstance, env: str) -> TaskInstance:
    """Register tasks on the dag.

    Args:
        dag (airflow.models.DAG): DAG instance to register tasks on.
        start (airflow.models.TaskInstance): Task instance all tasks will
            registered downstream from.

    Returns:
        airflow.models.TaskInstance: The last task node in this section.

    """
    trigger_sns_for_main_model = trigger_dag_run(
        start=start,
        env=env,
        trigger_dag_id="visits_estimation_ground_truth_supervised",
        source_table=[
            f"{Variable.get('env_project')}.{dag.params['visits_estimation_dataset']}.{dag.params['list_sns_pois_table']}",
            f"{Variable.get('env_project')}.{dag.params['visits_estimation_dataset']}.{dag.params['geospatial_output_table']}",
            f"{Variable.get('env_project')}.{dag.params['visits_estimation_dataset']}.{dag.params['model_input_olvin_table']}",
        ],
        destination_table=[
            f"{Variable.get('env_project')}.{dag.params['visits_estimation_dataset']}.{dag.params['ground_truth_output_core_table']}",
            f"{Variable.get('env_project')}.{dag.params['visits_estimation_dataset']}.{dag.params['monthly_visits_class_table']}",
            f"{Variable.get('env_project')}.{dag.params['visits_estimation_dataset']}.{dag.params['poi_class_business_table']}",
        ],
        retries=145,
        retry_delay=60*30
    )
    create_ts_brands_supervised = OlvinBigQueryOperator(
        task_id="create_ts_brands_supervised",
        query="{% include './bigquery/metrics/brand_agg/supervised.sql' %}",
        billing_tier="med",
    )
    mark_success_main_model = MarkSuccessOperator(
        task_id="mark_success_main_model", dag=dag
    )

    step = "ground_truth_supervised"
    with TaskGroup(group_id="yearly-seasonality") as yearly:

        create_yearly_seasonality_poi_list = OlvinBigQueryOperator(
            task_id="create_yearly_seasonality_poi_list",
            query="{% include './bigquery/ground_truth/yearly_queries/create_yearly_seasonality_poi_list.sql' %}",
            billing_tier="med",
        )

        create_yearly_seasonality_adj_input = OlvinBigQueryOperator(
            task_id="create_yearly_seasonality_adj_input",
            query="{% include './bigquery/ground_truth/yearly_queries/create_yearly_seasonality_adj_input.sql' %}",
            billing_tier="high",
        )

        create_yearly_seasonality_replacement = OlvinBigQueryOperator(
            task_id="create_yearly_seasonality_replacement",
            query="{% include './bigquery/ground_truth/yearly_queries/create_yearly_seasonality_replacement.sql' %}",
            billing_tier="high",
        )
        create_spark_input = OlvinBigQueryOperator(
            task_id="create_spark_input",
            query="{% include './bigquery/ground_truth/yearly_queries/create_spark_input.sql' %}",
        )

        create_spark_output = OlvinBigQueryOperator(
            task_id="create_spark_output",
            query="{% include './bigquery/ground_truth/yearly_queries/create_spark_output.sql' %}",
        )

        clear_gcs_input = GCSDeleteObjectsOperator(
            task_id="clear_gcs",
            bucket_name="{{ params['visits_estimation_bucket'] }}",
            prefix="supervised/yearly-seasonality/run_date={{ ds }}/input/",
        )

        clear_gcs_output = GCSDeleteObjectsOperator(
            task_id="clear_gcs_output",
            bucket_name="{{ params['visits_estimation_bucket'] }}",
            prefix="supervised/yearly-seasonality/run_date={{ ds }}/ouput/",
        )

        dataproc_create = BashOperator(
            task_id="dataproc-create",
            bash_command="{% include './bash/ground_truth/dataproc-create.sh' %}",
            env={
                "INSTANCE": "yearly-seasonality",
                "USERNAME": "olvin-com",
                "REPO": "pby-process-foot-traffic-adjustments",
                "BRANCH": "main",
                "GCP_AUTH_SECRET_NAME": "gh_access_foot_traffic_adjustments",
                "GCP_AUTH_SECRET_VERSION": "2",
                "PROJECT": f"{Variable.get('env_project')}",
                "SECRET_PROJECT": f"{Variable.get('prod_project')}",
                "REGION": "europe-west1",
                "MASTER_MACHINE_TYPE": "n2-standard-4",
                "MASTER_BOOT_DISK_SIZE": "500",
                "MASTER_NUM_LOCAL_SSDS": "1",
                "WORKER_NUM": "4",
                "WORKER_MACHINE_TYPE": "n2-standard-8",
                "WORKER_BOOT_DISK_SIZE": "500",
                "WORKER_NUM_LOCAL_SSDS": "1",
                "IMAGE_VERSION": "2.1-debian11",
                "MAX_IDLE": "30m",  # Change to 30m in prod
                "MAX_AGE": "185m",  # Change to time it takes to run +3 hours in prod
            },
        )

        dataproc_run = BashOperator(
            task_id="dataproc-run",
            bash_command="{% include './bash/ground_truth/dataproc-run.sh' %}",
            env={
                "INSTANCE": "yearly-seasonality",
                "PROJECT": f"{Variable.get('env_project')}",
                "REGION": "europe-west1",
                "PY_MAIN_FILE": f"gs://{{{{ params['visits_estimation_bucket'] }}}}/ground_truth/supervised/{{{{ params['py_main_file'] }}}}",
                "PY_DIST": f"gs://{{{{ params['visits_estimation_bucket'] }}}}/ground_truth/supervised/{{{{ params['py_dist'] }}}}",
                "GCS_OUTPUT_PATH": f"gs://{{{{ params['visits_estimation_bucket'] }}}}/ground_truth/supervised/yearly-seasonality/run_date={{{{ ds }}}}/output",
                "GCS_INPUT_PATH": f"gs://{{{{ params['visits_estimation_bucket'] }}}}/ground_truth/supervised/yearly-seasonality/run_date={{{{ ds }}}}/input",
                "INPUT_SCHEMA_BOOL": "true",
                "APPEND_MODE": "overwrite",
            },
        )

        (
            create_yearly_seasonality_poi_list
            >> create_yearly_seasonality_adj_input
            >> [clear_gcs_input, clear_gcs_output]
            >> create_spark_input
            >> dataproc_create
            >> dataproc_run
            >> create_spark_output
            >> create_yearly_seasonality_replacement
        )

    with TaskGroup(group_id="weekly-seasonality") as weekly:
        create_weekly_seasonality_poi_list = OlvinBigQueryOperator(
            task_id="create_weekly_seasonality_poi_list",
            query="{% include './bigquery/ground_truth/weekly_queries/create_weekly_seasonality_poi_list.sql' %}",
            billing_tier="high",
        )

        create_weekly_seasonality_replacement = OlvinBigQueryOperator(
            task_id="create_weekly_seasonality_replacement",
            query="{% include './bigquery/ground_truth/weekly_queries/create_weekly_seasonality_replacement.sql' %}",
            billing_tier="high",
        )

        create_weekly_seasonality_poi_list >> create_weekly_seasonality_replacement

    create_ts_brands_year_season = OlvinBigQueryOperator(
        task_id="create_ts_brands_year_season",
        query="{% include './bigquery/metrics/brand_agg/yearly_seasonality.sql' %}",
        billing_tier="med",
    )
    check_ground_truth_year_season_corrected_output = MarkSuccessOperator(
        task_id="check_ground_truth_year_season_corrected_output",
    )

    create_ts_brands_week_season = OlvinBigQueryOperator(
        task_id="create_ts_brands_week_season",
        query="{% include './bigquery/metrics/brand_agg/weekly_seasonality.sql' %}",
        billing_tier="med",
    )
    check_ground_truth_output_table_after_weekly_unbiasing = MarkSuccessOperator(
        task_id="check_ground_truth_output_table_after_weekly_unbiasing",
    )

    (
        mark_success_main_model
        >> yearly
        >> create_ts_brands_year_season
        >> check_ground_truth_year_season_corrected_output
        >> weekly
        >> create_ts_brands_week_season
        >> check_ground_truth_output_table_after_weekly_unbiasing
    )

    validations_start = EmptyOperator(task_id="main_model_validation_start")

    check_ground_truth_output_table_after_weekly_unbiasing >> validations_start

    main_model_validation = visits_estimation_validation(
        input_table=f"{Variable.get('env_project')}.{dag.params['visits_estimation_dataset']}.{dag.params['ground_truth_output_table']}",
        env=env,
        pipeline="visits_estimation",
        step=step,
        run_date="{{ ds }}",
        classes=["fk_sgbrands", "naics_code", "region", "top_category"],
        start_task=validations_start,
    )

    # Unbiased Validation
    # create a task to run bigquery code

    trigger_visualization_pipeline = TriggerDagRunOperator(
        task_id=f"trigger_visualization_pipeline_{step}",
        trigger_dag_id=f"{{{{ dag.dag_id }}}}" + "-visualizations",
        execution_date=f"{{{{ ds }}}} {datetime.now().time()}",
        conf={
            "step": f"{step}",
            "source_table": f"{Variable.get('env_project')}.{dag.params['visits_estimation_dataset']}.{dag.params['ground_truth_output_table']}",
        },
        wait_for_completion=True,
        dag=dag,
    )

    mark_success_main_model_validation = MarkSuccessOperator(
        task_id=f"mark_success_{step}_validation",
        on_failure_callback=check_dashboard_slack_alert(
            "U03BANPLXJR",
            data_studio_link="https://lookerstudio.google.com/reporting/07196097-8f5b-41d6-a6bf-b1b261bfe89d/page/p_j2j42t5i4c",
            message=f"Check metrics on {step}.",
        ),
        dag=dag,
    )

    (
        validations_start
        >> trigger_visualization_pipeline
        >> mark_success_main_model_validation
    )

    main_model_validation >> mark_success_main_model_validation

    (trigger_sns_for_main_model >> create_ts_brands_supervised >> mark_success_main_model)

    return mark_success_main_model_validation
