from __future__ import annotations

from airflow.models import DAG, TaskInstance
from airflow.providers.google.cloud.operators.bigquery import BigQueryValueCheckOperator
from airflow.utils.task_group import TaskGroup
from common.operators.bigquery import SNSBigQueryOperator
from common.operators.triggers import SNSTriggerDAGRunOperator


def register(start: TaskInstance, dag: DAG) -> TaskGroup:
    TRIGGER_DAG_ID = f"supervised-daily-inference-{dag.params['stage']}"

    with TaskGroup(group_id="model-inference") as group:
        trigger = SNSTriggerDAGRunOperator(
            task_id="trigger",
            trigger_dag_id=TRIGGER_DAG_ID,
            execution_date="{{ ts }}",
            reset_dag_run=True,
        )
        start >> trigger

        wait = BigQueryValueCheckOperator(
            task_id="wait",
            sql="{% include './include/bigquery/wait.sql' %}",
            pass_value=True,
            use_legacy_sql=False,
            retries=60,
            retry_delay=60 * 2,
            params={"trigger-dag-id": TRIGGER_DAG_ID},
        )
        trigger >> wait

        load_output = SNSBigQueryOperator(
            task_id="load",
            query="{% include './include/bigquery/model_inference/load.sql' %}",
        )
        wait >> load_output

    start >> group

    return group
