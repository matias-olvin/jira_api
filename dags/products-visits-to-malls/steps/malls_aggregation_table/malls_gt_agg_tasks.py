from __future__ import annotations

from airflow.models import TaskInstance
from common.operators.bigquery import SNSBigQueryOperator


def register(start: TaskInstance) -> TaskInstance:

    create_malls_gt_agg_table_in_sns = SNSBigQueryOperator(
        task_id="create_malls_gt_agg_table_in_sns",
        query="{% include './include/bigquery/malls_aggregation_table/create_malls_gt_agg_table_in_sns.sql' %}",
    )

    start >> create_malls_gt_agg_table_in_sns

    return create_malls_gt_agg_table_in_sns
