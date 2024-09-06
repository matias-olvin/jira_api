from __future__ import annotations

from airflow.models import DAG, TaskInstance
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryValueCheckOperator
from airflow.utils.task_group import TaskGroup
from common.operators.bigquery import OlvinBigQueryOperator, SNSBigQueryOperator
from common.operators.triggers import SNSTriggerDAGRunOperator


def register(start: TaskInstance, dag: DAG) -> TaskGroup:
    TRIGGER_DAG_ID = "supervised-daily-monitoring-input-factors"

    with TaskGroup(group_id="model-input") as group:
        check_sns = BigQueryValueCheckOperator(
            task_id="check-sns",
            sql="{% include './include/bigquery/model_input/check-sns.sql' %}",
            pass_value=True,
            use_legacy_sql=False,
            retries=10,
        )

        gt_business = SNSBigQueryOperator(
            task_id="gt-business",
            query="{% include './include/bigquery/model_input/gt-business.sql' %}",
        )
        factor_business = SNSBigQueryOperator(
            task_id="factor-business",
            query="{% include './include/bigquery/model_input/factor-business.sql' %}",
        )
        check_sns >> gt_business >> factor_business

        gt_location = SNSBigQueryOperator(
            task_id="gt-location",
            query="{% include './include/bigquery/model_input/gt-location.sql' %}",
        )
        factor_location = SNSBigQueryOperator(
            task_id="factor-location",
            query="{% include './include/bigquery/model_input/factor-location.sql' %}",
        )
        check_sns >> gt_location >> factor_location

        metrics = OlvinBigQueryOperator(
            task_id="metrics",
            query="{% include './include/bigquery/model_input/metrics.sql' %}",
        )
        [factor_business, factor_location] >> metrics

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
        metrics >> trigger

        wait = (
            BigQueryValueCheckOperator(
                task_id="wait",
                sql="{% include './include/bigquery/wait.sql' %}",
                pass_value=True,
                use_legacy_sql=False,
                retries=60,
                retry_delay=60 * 2,
                params={"trigger-dag-id": TRIGGER_DAG_ID},
            )
            if dag.params["stage"] == "production"
            else EmptyOperator(task_id="wait")
        )
        trigger >> wait

        model_input = SNSBigQueryOperator(
            task_id="model-input",
            query="{% include './include/bigquery/model_input/input.sql' %}",
        )
        wait >> model_input

    start >> group

    return group
