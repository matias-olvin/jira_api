"""
DAG ID: visits_pipeline_all
"""

from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator


def register(dag, start):
    """Register tasks on the dag.

    Args:
        dag (airflow.models.DAG): DAG instance to register tasks on.
        start (airflow.models.TaskInstance): Task instance all tasks will
            registered downstream from.

    Returns:
        airflow.models.TaskInstance: The last task node in this section.
    """

    # For now, only trigger bloomberg_feed if latest DagRun.

    # If we decide to re-run bloomberg-feed when backfilling then the LatestOnlyOperator
    # needs to be removed.

    # In Airflow 1.X bloomberg_feed will need to be manually re-run.

    # In Airflow 2.X TriggerDagRunOperator has parameter reset_dag_runs, which
    # should be set to True if we want to re-run bloomberg_feed after backfill

    skip_trigger_if_backfill = LatestOnlyOperator(
        task_id="skip_trigger_if_backfill",
        wait_for_downstream=False,
        depends_on_past=False,
        dag=dag,
    )
    trigger_bloomberg_feed = TriggerDagRunOperator(
        task_id="trigger_bloomberg_feed",
        trigger_dag_id="products-feeds-bloomberg",
        execution_date="{{ ds }}",
        depends_on_past=False,
        wait_for_downstream=False,
    )

    start >> skip_trigger_if_backfill >> trigger_bloomberg_feed

    return trigger_bloomberg_feed
