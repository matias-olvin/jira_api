from __future__ import annotations

from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG, TaskInstance

from common.operators.exceptions import MarkSuccessOperator
from common.operators.triggers import SNSTriggerDAGRunOperator

def register(start: TaskInstance, dag: DAG) -> TaskInstance:
    
    mark_success = MarkSuccessOperator(task_id="mark_success_to_confirm_sns_final_transfer_and_backfill")

    trigger = SNSTriggerDAGRunOperator(
        task_id="trigger_post_smc_sns_transfer_and_backfill",
        trigger_dag_id="post_smc_sns"
    )

    start >> trigger >> mark_success

    return mark_success