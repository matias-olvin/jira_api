from __future__ import annotations

from airflow.models import DAG, TaskInstance, Variable
from airflow.utils.task_group import TaskGroup
from common.operators.bigquery import OlvinBigQueryOperator, SNSBigQueryOperator


def register(start: TaskInstance, dag: DAG) -> TaskGroup:
    params = {
        "bigquery-project": (
            Variable.get("almanac_project")
            if dag.params["stage"] == "production"
            else Variable.get("env_project")
        ),
        "accessible-dataset": (
            dag.params["accessible-by-olvin-almanac-dataset"]
            if dag.params["stage"] == "production"
            else dag.params["accessible-by-olvin-dataset"]
        ),
    }

    with TaskGroup(group_id="final-visits") as group:
        sgplacedailyvisitsraw = SNSBigQueryOperator(
            task_id="sgplacedailyvisitsraw",
            query="{% include './include/bigquery/final_visits/sgplacedailyvisitsraw.sql' %}",
            params=params,
        )

        metrics_dod = SNSBigQueryOperator(
            task_id="metrics_dod",
            query="{% include './include/bigquery/final_visits/metrics-dod.sql' %}",
            params=params,
        )
        metrics_daily_diff = SNSBigQueryOperator(
            task_id="metrics_daily_diff",
            query="{% include './include/bigquery/final_visits/metrics-daily-diff.sql' %}",
            params=params,
        )
        metrics_yoy = SNSBigQueryOperator(
            task_id="metrics_yoy",
            query="{% include './include/bigquery/final_visits/metrics-yoy.sql' %}",
            params=params,
        )
        metrics = OlvinBigQueryOperator(
            task_id="metrics",
            query="{% include './include/bigquery/final_visits/metrics.sql' %}",
            params=params,
        )
        # all metrics merge into same table, so run metrics in series
        (
            sgplacedailyvisitsraw
            >> metrics_dod
            >> metrics_daily_diff
            >> metrics_yoy
            >> metrics
        )

        sgplacemonthlyvisitsraw = SNSBigQueryOperator(
            task_id="sgplacemonthlyvisitsraw",
            query="{% include './include/bigquery/final_visits/sgplacemonthlyvisitsraw.sql' %}",
            params=params,
        )
        metrics >> sgplacemonthlyvisitsraw

        # SGPlaceDailyVisitsRaw and SGPlaceMonthlyVisitsRaw need to read from tables
        # in visits_estimation_ground_truth_supervised_{{ stage }} dataset. Hence,
        # they need to be run in SNS Project. The rest of the tasks can be run in
        # Olvin Project, once those tables have been migrated to from SNS Project.
        migrate = OlvinBigQueryOperator(
            task_id="migrate",
            query="{% include './include/bigquery/final_visits/migrate.sql' %}",
            params=params,
        )
        sgplacemonthlyvisitsraw >> migrate

        sgplaceranking = OlvinBigQueryOperator(
            task_id="sgplaceranking",
            query="{% include './include/bigquery/final_visits/sgplaceranking.sql' %}",
            params=params,
        )
        migrate >> sgplaceranking

    start >> group

    with TaskGroup(group_id="materialized-views") as group_mv:
        OlvinBigQueryOperator(
            task_id="mv_sgplacedailyvisitsraw",
            query="{% include './include/bigquery/final_visits/mv_sgplacedailyvisitsraw.sql' %}",
            params=params,
        )
        OlvinBigQueryOperator(
            task_id="mv_sgplacemonthlyvisitsraw",
            query="{% include './include/bigquery/final_visits/mv_sgplacemonthlyvisitsraw.sql' %}",
            params=params,
        )
        OlvinBigQueryOperator(
            task_id="sgbranddailyvisits",
            query="{% include './include/bigquery/final_visits/sgbranddailyvisits.sql' %}",
            params=params,
        )
        OlvinBigQueryOperator(
            task_id="sgbrandmonthlyvisits",
            query="{% include './include/bigquery/final_visits/sgbrandmonthlyvisits.sql' %}",
            params=params,
        )
        OlvinBigQueryOperator(
            task_id="sgbrandstatedailyvisits",
            query="{% include './include/bigquery/final_visits/sgbrandstatedailyvisits.sql' %}",
            params=params,
        )
        OlvinBigQueryOperator(
            task_id="sgbrandstatemonthlyvisits",
            query="{% include './include/bigquery/final_visits/sgbrandstatemonthlyvisits.sql' %}",
            params=params,
        )
        OlvinBigQueryOperator(
            task_id="version",
            query="{% include './include/bigquery/final_visits/version.sql' %}",
            params=params,
        )

    group >> group_mv

    return group_mv
