from airflow.models import DAG, TaskInstance
from common.operators.bigquery import OlvinBigQueryOperator
from common.utils import callbacks


def register(start: TaskInstance, dag: DAG, output_folder: str) -> TaskInstance:
    """Register tasks on the dag.

    Args:
        start (airflow.models.TaskInstance): Task instance all tasks will
            registered downstream from.
        dag (airflow.models.DAG): DAG instance to register tasks on.
        output_folder (str): Name of output folder.
    Returns:
        airflow.models.TaskInstance: The last task node in this section.
    """
    tasks = list()

    load_hourly_grid_weather = OlvinBigQueryOperator(
        task_id=f"load_hourly_grid_weather_{output_folder}",
        query=f"{{% include './include/bigquery/load_hourly_grid_weather_{output_folder}.sql' %}}",
    )

    start >> load_hourly_grid_weather

    query_check_hour = OlvinBigQueryOperator(
        task_id="query_check_hour",
        query="{% include './include/bigquery/regressor_update_check_hour.sql' %}",
    )

    load_hourly_grid_weather >> query_check_hour

    check_hours_loaded_span = OlvinBigQueryOperator(
        task_id="check_hours_loaded_span",
        query="{% include './include/bigquery/verification/check_hours_difference.sql' %}",
        on_failure_callback=callbacks.task_fail_slack_alert(
            "U03BANPLXJR", channel="prod"  # Carlos
        ),
    )
    check_hours_loaded_max = OlvinBigQueryOperator(
        task_id="check_hours_loaded_max",
        query="{% include './include/bigquery/verification/check_max_hour.sql' %}",
        on_failure_callback=callbacks.task_fail_slack_alert(
            "U03BANPLXJR", channel="prod"  # Carlos
        ),
    )
    check_hours_loaded_min = OlvinBigQueryOperator(
        task_id="check_hours_loaded_min",
        query="{% include './include/bigquery/verification/check_min_hour.sql' %}",
        on_failure_callback=callbacks.task_fail_slack_alert(
            "U03BANPLXJR", channel="prod"  # Carlos
        ),
    )
    check_num_hours_loaded_per_region = OlvinBigQueryOperator(
        task_id="check_num_hours_loaded_per_region",
        query="{% include './include/bigquery/verification/num_hours_per_region.sql' %}",
        on_failure_callback=callbacks.task_fail_slack_alert(
            "U03BANPLXJR", channel="prod"  # Carlos
        ),
    )
    check_num_regions_loaded_per_hour = OlvinBigQueryOperator(
        task_id="check_num_regions_loaded_per_hour",
        query="{% include './include/bigquery/verification/num_regions_per_hour.sql' %}",
        on_failure_callback=callbacks.task_fail_slack_alert(
            "U03BANPLXJR", channel="prod"  # Carlos
        ),
    )

    query_check_hour >> [
        check_hours_loaded_span,
        check_hours_loaded_max,
        check_hours_loaded_min,
        check_num_hours_loaded_per_region,
        check_num_regions_loaded_per_hour,
    ]

    query_extract_regressor = OlvinBigQueryOperator(
        task_id=f"query_extract_regressor_{output_folder}",
        query=f"{{% include './include/bigquery/regressor_extract_update_{output_folder}.sql' %}}",
    )

    query_drop_nulls = OlvinBigQueryOperator(
        task_id=f"query_drop_nulls_{output_folder}",
        query=f"{{% include './include/bigquery/query_drop_nulls_{output_folder}.sql' %}}",
    )

    (
        [
            check_hours_loaded_span,
            check_hours_loaded_max,
            check_hours_loaded_min,
            check_num_hours_loaded_per_region,
            check_num_regions_loaded_per_hour,
        ]
        >> query_extract_regressor
        >> query_drop_nulls
    )

    query_insert_regressor = OlvinBigQueryOperator(
        task_id=f"query_insert_regressor_{output_folder}",
        query=f"{{% include './include/bigquery/regressor_insert_update_{output_folder}.sql' %}}",
        billing_tier="med",
    )

    query_drop_nulls >> query_insert_regressor

    tasks.append(query_insert_regressor)

    return tasks
