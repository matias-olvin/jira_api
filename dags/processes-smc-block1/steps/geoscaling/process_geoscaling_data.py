import pendulum
from airflow.operators.empty import EmptyOperator
import pandas as pd
from dateutil.relativedelta import relativedelta
from airflow.utils.task_group import TaskGroup
from airflow.models import TaskInstance, DAG
from common.operators.bigquery import OlvinBigQueryOperator


def register(start: TaskInstance, dag: DAG) -> TaskGroup:
    """Register tasks on the dag.

    Args:
        start (airflow.models.TaskInstance): Task instance all tasks will
        registered downstream from.
        dag (airflow.models.DAG): DAG instance to register tasks on.

    Returns:
        airflow.utils.task_group.TaskGroup: The last task node in this section.
    """
    with TaskGroup(group_id="process_geoscaling_data_task_group") as group:
        query_create_quality_ml_input = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="query_create_quality_ml_input",
            query= "{% include './include/bigquery/geoscaling/create_quality_ml_input.sql' %}"
        )

        years_compute = pd.date_range(
            "2018-01-01", pendulum.today().format("YYYY-MM-DD"), freq="YS", inclusive="left"
        )

        quality_input_tasks_params = list()

        start_quality_input_backfill = EmptyOperator(task_id="start_quality_input_backfill")
        for year_start in years_compute:
            date_start = year_start.strftime("%Y-%m-%d")

            date_end = (year_start + relativedelta(years=1)).strftime("%Y-%m-%d")

            quality_input_tasks_params.append(
                {
                "geoscaling_input_start_date": date_start,
                "geoscaling_input_end_date": date_end
                }
            )

        quality_input_tasks = OlvinBigQueryOperator.partial(
            project_id="{{ var.value.dev_project_id }}",
            task_id="query_quality_ml_input",
            billing_tier="highest",
            query="{% include './include/bigquery/geoscaling/quality_ml_input.sql' %}"
        ).expand(params=quality_input_tasks_params)

        geoscaling_data_end = EmptyOperator(task_id="geoscaling_data_end")

        (
            start
            >> query_create_quality_ml_input
            >> start_quality_input_backfill
            >> quality_input_tasks
            >> geoscaling_data_end
        )

    return group
