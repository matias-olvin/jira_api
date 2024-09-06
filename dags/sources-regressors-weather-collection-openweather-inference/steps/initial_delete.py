from airflow.models import DAG, TaskInstance
from airflow.models.dag import get_last_dagrun
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.session import provide_session
from datetime import datetime, timedelta


def register(start: TaskInstance, dag: DAG) -> TaskInstance:
    """Register tasks on the dag.

    Args:
        start (airflow.models.TaskInstance): Task instance all tasks will
            registered downstream from.
        dag (airflow.models.DAG): DAG instance to register tasks on.

    Returns:
        airflow.models.TaskInstance
    """

    def _return_ds_weather_collection_openweather_inference(execution_date):
        # this dag is scheduled to run every quarter, so the run_date would be the ds + 3 months
        if isinstance(execution_date, str):
            execution_date = datetime.strptime(execution_date, "%Y-%m-%d")
        elif isinstance(execution_date, datetime):
            pass
        else:
            raise TypeError("execution_date must be a string or a datetime object.")


        if execution_date.day != 1:
            raise ValueError("The date is not the first of the month.")

        ds_month = execution_date.month
        run_date_month = ds_month + 3

        run_date = execution_date.replace(month=run_date_month)

        # subtract one day in order obtain ds of run_date (scheduled daily so run_date = ds + 1)
        ds_of_run_date = run_date - timedelta(days=1)

        return ds_of_run_date

    @provide_session
    def exec_date_fn(execution_date, session=None, **kwargs):
        """
        If the last DagRun for sources-regressors-weather-collection-openweather-update is over (or doesn't exist) ie returns None, 
        the execution date of the current run date of this dag
        """
        openweather_update_id = get_last_dagrun(
            dag_id="{{ params['dag-sources-regressors-weather-collection-openweather-update'] }}",
            session=session,
        )

        if not openweather_update_id.execution_date:
            return _return_ds_weather_collection_openweather_inference(execution_date)

        return openweather_update_id.execution_date

    wait_for_openweather_update_completion = ExternalTaskSensor(
        task_id="wait_for_openweather_update_completion",
        external_dag_id="{{ params['dag-sources-regressors-weather-collection-openweather-update'] }}",
        external_task_id="end",
        check_existence=True,
        execution_date_fn=exec_date_fn,
        mode="reschedule",
        poke_interval=60,
    )

    delay = BashOperator(task_id="delay", dag=dag, bash_command="sleep 10")

    delete_initial_inference = GCSDeleteObjectsOperator(
        task_id="delete_initial_inference",
        depends_on_past=False,
        dag=dag,
        bucket_name="{{ params['weather_bucket'] }}",
        prefix="input_data/inference_folder/",
    )

    start >> wait_for_openweather_update_completion >> delay >> delete_initial_inference

    return delete_initial_inference
