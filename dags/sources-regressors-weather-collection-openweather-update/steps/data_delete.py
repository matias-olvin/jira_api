from airflow.models import DAG, TaskInstance
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator


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
    tasks = []

    delete_final_hourly_neighbourhoods = GCSDeleteObjectsOperator(
        task_id=f"delete_final_hourly_neighbourhoods_{output_folder}",
        depends_on_past=False,
        dag=dag,
        bucket_name="{{ params['weather_bucket'] }}",
        prefix=f"{output_folder}/GOLD/neighbourhoods_hourly/",
    )
    start >> delete_final_hourly_neighbourhoods
    tasks.append(delete_final_hourly_neighbourhoods)

    delete_final_hourly_csds = GCSDeleteObjectsOperator(
        task_id=f"delete_final_hourly_csds_{output_folder}",
        depends_on_past=False,
        dag=dag,
        bucket_name="{{ params['weather_bucket'] }}",
        prefix=f"{output_folder}/GOLD/csds_hourly/",
    )
    start >> delete_final_hourly_csds
    tasks.append(delete_final_hourly_csds)

    delete_final_hourly_zipcodes = GCSDeleteObjectsOperator(
        task_id=f"delete_final_hourly_zipcodes_{output_folder}",
        depends_on_past=False,
        dag=dag,
        bucket_name="{{ params['weather_bucket'] }}",
        prefix=f"{output_folder}/GOLD/zipcodes_hourly/",
    )
    start >> delete_final_hourly_zipcodes
    tasks.append(delete_final_hourly_zipcodes)

    delete_final_daily_neighbourhoods = GCSDeleteObjectsOperator(
        task_id=f"delete_final_daily_neighbourhoods_{output_folder}",
        depends_on_past=False,
        dag=dag,
        bucket_name="{{ params['weather_bucket'] }}",
        prefix=f"{output_folder}/GOLD/neighbourhoods_daily/",
    )
    start >> delete_final_daily_neighbourhoods
    tasks.append(delete_final_daily_neighbourhoods)

    delete_final_daily_csds = GCSDeleteObjectsOperator(
        task_id=f"delete_final_daily_csds_{output_folder}",
        depends_on_past=False,
        dag=dag,
        bucket_name="{{ params['weather_bucket'] }}",
        prefix=f"{output_folder}/GOLD/csds_daily/",
    )
    start >> delete_final_daily_csds
    tasks.append(delete_final_daily_csds)

    delete_final_daily_zipcodes = GCSDeleteObjectsOperator(
        task_id=f"delete_final_daily_zipcodes_{output_folder}",
        depends_on_past=False,
        dag=dag,
        bucket_name="{{ params['weather_bucket'] }}",
        prefix=f"{output_folder}/GOLD/zipcodes_daily/",
    )
    start >> delete_final_daily_zipcodes
    tasks.append(delete_final_daily_zipcodes)

    return tasks
