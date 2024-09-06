from datetime import datetime, timedelta

from airflow import AirflowException
from airflow.models import DAG, TaskInstance
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor


def register(start: TaskInstance, dag: DAG) -> TaskInstance:
    """
    Register tasks on the dag.

    Args:
        start (airflow.models.TaskInstance): Task instance all tasks will
            registered downstream from.
        dag (airflow.models.DAG): DAG instance to register tasks on.

    Returns:
        airflow.models.TaskInstance: The last task node in this section.
    """

    def task_to_fail():
        """
        Task that will fail.
        """
        raise AirflowException("This task must be manually set to success to continue.")

    def exec_delta_fn(execution_date, context):
        ti = context["ti"]
        source_exec_date = execution_date.replace(tzinfo=None)
        target_exec_date = datetime.strptime(
            f"{ti.xcom_pull(task_ids='push_execution_date')}T00:00:00",
            "%Y-%m-%dT00:00:00",
        )
        diff = source_exec_date - target_exec_date
        diff_seconds = diff.total_seconds()

        return execution_date - timedelta(seconds=diff_seconds)

    def trigger_visitor_destination(start=start):
        """
        Wait for a task to be manually set to success, then trigger visits_estimation.\n
        If not set to success, trigger visits_estimation_postgres_update.
        """
        visitor_destination_dag_id = dag.params[
            "dag-processes-monthly-update-visitor-destination"
        ]
        # This task will go into up_for_retry once, then fail after the retry_delay.
        # Task must be manunally marked as success in the UI to progress.
        wait_until_ready = PythonOperator(
            task_id=f"mark_success_to_update_{visitor_destination_dag_id}",
            python_callable=task_to_fail,
            retries=1,
            retry_delay=timedelta(weeks=6),
            dag=dag,
        )

        # If upstream Task is 'success' then Trigger visits_estimation.
        # If upstream Task is 'failed' this Task will be skipped.
        trigger_pipeline = TriggerDagRunOperator(
            task_id=f"trigger_{visitor_destination_dag_id}",
            trigger_dag_id=visitor_destination_dag_id,
            execution_date="{{ task_instance.xcom_pull(task_ids='push_execution_date') }}",
            trigger_rule="one_success",
        )

        wait_for_pipeline = ExternalTaskSensor(
            task_id=f"wait_for_{visitor_destination_dag_id}",
            external_dag_id=visitor_destination_dag_id,
            execution_date_fn=exec_delta_fn,
            poke_interval=60 * 60 * 2,
            mode="reschedule",
        )

        start >> wait_until_ready >> trigger_pipeline >> wait_for_pipeline

        return wait_for_pipeline

    def trigger_void_preprocess(start):
        """
        Wait for a task to be manually set to success, then trigger visits_estimation.\n
        If not set to success, trigger visits_estimation_postgres_update.
        """
        void_preprocess_dag_id = dag.params[
            "dag-processes-monthly-update-void-market-preprocess"
        ]
        # This task will go into up_for_retry once, then fail after the retry_delay.
        # Task must be manunally marked as success in the UI to progress.
        wait_until_ready = PythonOperator(
            task_id=f"mark_success_to_update_{void_preprocess_dag_id}",
            python_callable=task_to_fail,
            retries=1,
            retry_delay=timedelta(weeks=6),
            dag=dag,
        )

        # If upstream Task is 'success' then Trigger visits_estimation.
        # If upstream Task is 'failed' this Task will be skipped.
        trigger_pipeline = TriggerDagRunOperator(
            task_id=f"trigger_{void_preprocess_dag_id}",
            trigger_dag_id=void_preprocess_dag_id,
            execution_date="{{ task_instance.xcom_pull(task_ids='push_execution_date') }}",
            trigger_rule="one_success",
        )

        wait_for_pipeline = ExternalTaskSensor(
            task_id=f"wait_for_{void_preprocess_dag_id}",
            external_dag_id=void_preprocess_dag_id,
            execution_date_fn=exec_delta_fn,
            poke_interval=60 * 60 * 2,
            mode="reschedule",
        )

        start >> wait_until_ready >> trigger_pipeline >> wait_for_pipeline

        return wait_for_pipeline

    def trigger_visits_estimation(start):
        """
        Wait for a task to be manually set to success, then trigger visits_estimation.\n
        If not set to success, trigger visits_estimation_postgres_update.
        """

        pipelines = {
            "success": f"{dag.params['dag-processes-monthly-update-visits-estimation']}",
            "failed": f"{dag.params['dag-processes-monthly-update-visits-estimation-postgres']}",
        }

        # This task will go into up_for_retry once, then fail after the retry_delay.
        # Task must be manunally marked as success in the UI to progress.
        wait_until_ready = PythonOperator(
            task_id=f"mark_success_to_update_{pipelines['success']}",
            python_callable=task_to_fail,
            retries=1,
            retry_delay=timedelta(weeks=6),
            dag=dag,
        )
        # If upstream Task is 'success' then Trigger visits_estimation.
        # If upstream Task is 'failed' this Task will be skipped.
        trigger_pipeline = TriggerDagRunOperator(
            task_id=f"trigger_{pipelines['success']}",
            trigger_dag_id=pipelines["success"],
            execution_date="{{ task_instance.xcom_pull(task_ids='push_execution_date') }}",
            trigger_rule="one_success",
        )
        wait_for_pipeline = ExternalTaskSensor(
            task_id=f"wait_for_{pipelines['success']}",
            external_dag_id=pipelines["success"],
            execution_date_fn=exec_delta_fn,
            poke_interval=60 * 60 * 2,
            mode="reschedule",
        )
        # If upstream Task is 'failed' then Trigger visits_estimation_postgres_update.
        # If upstream Task is 'success' this Task will be skipped.
        trigger_postgres_pipeline = TriggerDagRunOperator(
            task_id=f"trigger_{pipelines['failed']}",
            trigger_dag_id=pipelines["failed"],
            execution_date="{{ task_instance.xcom_pull(task_ids='push_execution_date') }}",
            trigger_rule="one_failed",
        )
        # postres pipeline run in both cases (triggered in visits_estimation)
        wait_for_postgres_pipeline = ExternalTaskSensor(
            task_id=f"wait_for_{pipelines['failed']}",
            external_dag_id=pipelines["failed"],
            execution_date_fn=exec_delta_fn,
            poke_interval=60 * 60 * 2,
            mode="reschedule",
            trigger_rule="one_success",
        )

        (
            start
            >> wait_until_ready
            >> trigger_pipeline
            >> wait_for_pipeline
            >> wait_for_postgres_pipeline
        )
        (
            start
            >> wait_until_ready
            >> trigger_postgres_pipeline
            >> wait_for_postgres_pipeline
        )

        return wait_for_postgres_pipeline

    def trigger_cameo(start):
        cameo_dag_id = dag.params["dag-processes-monthly-update-cameo-profiles"]

        trigger_pipeline = TriggerDagRunOperator(
            task_id=f"trigger_{cameo_dag_id}",
            trigger_dag_id=f"{cameo_dag_id}",
            execution_date="{{ task_instance.xcom_pull(task_ids='push_execution_date') }}",
            trigger_rule="one_success",
        )
        wait_for_pipeline = ExternalTaskSensor(
            task_id=f"wait_for_{cameo_dag_id}",
            external_dag_id=f"{cameo_dag_id}",
            execution_date_fn=exec_delta_fn,
            poke_interval=60 * 60 * 2,
            mode="reschedule",
        )
        start >> trigger_pipeline >> wait_for_pipeline

        return wait_for_pipeline

    def trigger_demographics(start):
        """
        Wait for a task to be manually set to success, then trigger pipeline.
        """
        demo_pipeline = dag.params["dag-processes-monthly-update-demographics"]

        wait_until_ready = PythonOperator(
            task_id=f"mark_success_to_update_{demo_pipeline}",
            python_callable=task_to_fail,
            retries=1,
            retry_delay=timedelta(weeks=6),
            dag=dag,
        )

        trigger_demographics = TriggerDagRunOperator(
            task_id=f"trigger_{demo_pipeline}",
            trigger_dag_id=f"{demo_pipeline}",
            execution_date="{{ task_instance.xcom_pull(task_ids='push_execution_date') }}",
            trigger_rule="one_success",
        )
        wait_for_demogrpahics = ExternalTaskSensor(
            task_id=f"wait_for_{demo_pipeline}",
            external_dag_id=f"{demo_pipeline}",
            execution_date_fn=exec_delta_fn,
            poke_interval=60 * 60 * 2,
            mode="reschedule",
        )
        wait_until_ready_post_demographics = PythonOperator(
            task_id=f"mark_success_to_complete_{demo_pipeline}",
            python_callable=task_to_fail,
            retries=1,
            retry_delay=timedelta(weeks=6),
            dag=dag,
        )
        (
            start
            >> wait_until_ready
            >> trigger_demographics
            >> wait_for_demogrpahics
            >> wait_until_ready_post_demographics
        )

        return wait_until_ready_post_demographics

    def trigger_agg_stats(start):

        agg_stats_dag_id = dag.params["dag-processes-monthly-update-agg-stats"]
        trigger_agg = TriggerDagRunOperator(
            task_id=f"trigger_{agg_stats_dag_id}",
            trigger_dag_id=f"{agg_stats_dag_id}",
            execution_date="{{ task_instance.xcom_pull(task_ids='push_execution_date') }}",
        )
        wait_for_agg = ExternalTaskSensor(
            task_id=f"wait_for_{agg_stats_dag_id}",
            external_dag_id=f"{agg_stats_dag_id}",
            execution_date_fn=exec_delta_fn,
            poke_interval=60 * 60 * 2,
            mode="reschedule",
        )

        start >> trigger_agg >> wait_for_agg

        return wait_for_agg

    triggers_end = EmptyOperator(
        task_id="triggers_end",
    )

    demo_end = trigger_demographics(start=start)
    cameo_end = trigger_cameo(start=demo_end)

    trigger_visits_estimation_end = trigger_visits_estimation(start=start)

    trigger_visitor_destination_end = trigger_visitor_destination(start=start)

    agg_stats_end = trigger_agg_stats(
        start=[
            cameo_end,
            trigger_visits_estimation_end,
            trigger_visitor_destination_end,
        ]
    )

    trigger_void_preprocess_end = trigger_void_preprocess(start=start)

    [agg_stats_end, trigger_void_preprocess_end] >> triggers_end

    return triggers_end
