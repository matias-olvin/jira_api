from datetime import datetime, timedelta

from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.dummy_operator import DummyOperator

# from airflow.models.baseoperator import chain


def register(dag, start, pipeline):
    """
    Register tasks on the dag

    Args:
        dag (airflow.models.DAG): DAG instance to register tasks on.
        start (airflow.models.TaskInstance): Task instance all tasks will
        be registered downstream from.

    Returns:
        airflow.models.TaskInstance: The last task node in this section.
    """
    # trigger backfill pipeline
    backfill_end = DummyOperator(task_id="backfill_end")

    dag_config = Variable.get("visits_backfill_dates", deserialize_json=True)
    start_date = datetime.strptime(dag_config["start"], "%Y-%m-%d")
    end_date = datetime.strptime(dag_config["end"], "%Y-%m-%d")
    delta = end_date - start_date

    prev_task = start
    for i in range(delta.days + 1):
        # for each day in range, get date and Trigger DagRun.
        day = (start_date + timedelta(days=i)).strftime("%Y-%m-%d")
        day_nodash = day.replace("-", "")
        trigger_date = f"{day}T03:00:00"

        trigger = TriggerDagRunOperator(
            task_id=f"trigger_{day_nodash}",
            trigger_dag_id=pipeline,
            execution_date=f"{trigger_date}",
            conf={"mode": "backfill"},
            reset_dag_run=True,
            wait_for_completion=True,
            poke_interval=60 * 2,
        )
        prev_task >> trigger

        prev_task = trigger

    trigger >> backfill_end

    return backfill_end
