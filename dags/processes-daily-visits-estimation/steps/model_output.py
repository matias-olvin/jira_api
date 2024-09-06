from __future__ import annotations

from airflow.models import DAG, TaskInstance, Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryValueCheckOperator
from airflow.utils.task_group import TaskGroup
from common.operators.bigquery import OlvinBigQueryOperator, SNSBigQueryOperator
from common.operators.triggers import SNSTriggerDAGRunOperator


def register(start: TaskInstance, dag: DAG) -> TaskGroup:
    TRIGGER_DAG_ID = "supervised-daily-monitoring-output-factors"
    params = {
        "trigger-dag-id": TRIGGER_DAG_ID,
        "bigquery-project": (
            Variable.get("env_project")
            if dag.params["stage"] == "production"
            else Variable.get("almanac_project")
        ),
        "accessible-dataset": (
                dag.params["accessible-by-olvin-almanac-dataset"]
                if dag.params["stage"] == "production"
                else dag.params["accessible-by-olvin-dataset"]
        ),
    }

    with TaskGroup(group_id="model-output") as group:
        factor = SNSBigQueryOperator(
            task_id="factor",
            query="{% include './include/bigquery/model_output/factor.sql' %}",
        )

        trigger = (
            SNSTriggerDAGRunOperator(
                task_id="trigger",
                trigger_dag_id=TRIGGER_DAG_ID,
                execution_date="{{ ts }}",
                reset_dag_run=True,
            )
            if dag.params["stage"] == "production"
            else EmptyOperator(task_id="trigger")
        )
        factor >> trigger

        wait = (
            BigQueryValueCheckOperator(
                task_id="wait",
                sql="{% include './include/bigquery/wait.sql' %}",
                pass_value=True,
                use_legacy_sql=False,
                retries=60,
                retry_delay=60 * 2,
                params=params,
            )
            if dag.params["stage"] == "production"
            else EmptyOperator(task_id="wait")
        )
        trigger >> wait

        visits = SNSBigQueryOperator(
            task_id="visits",
            query="{% include './include/bigquery/model_output/visits.sql' %}",
            params=params,
        )
        wait >> visits

        brand_adjustment = SNSBigQueryOperator(
            task_id="brand_adjustment",
            query="{% include './include/bigquery/model_output/brand_adjustment.sql' %}",
            params=params,
        )
        visits >> brand_adjustment

        metrics = OlvinBigQueryOperator(
            task_id="metrics",
            query="{% include './include/bigquery/model_output/metrics.sql' %}",
        )
        brand_adjustment >> metrics

    start >> group

    return group
