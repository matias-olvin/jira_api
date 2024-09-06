from __future__ import annotations

from airflow.models import TaskInstance, Variable
from airflow.utils.task_group import TaskGroup
from common.operators.bigquery import OlvinBigQueryOperator, SNSBigQueryOperator
from common.utils import formatting, queries


def register(start: TaskInstance) -> TaskInstance:

    create_visits_to_malls_daily_task = SNSBigQueryOperator(
        task_id="create_visits_to_malls_daily_table_in_sns",
        query="{% include './include/bigquery/scaling_to_all_US_malls/create_visits_to_malls_daily.sql' %}",
    )

    start >> create_visits_to_malls_daily_task

    with TaskGroup(group_id="copy_visits_to_mall_daily_table_to_olvin") as group:

        source = "{{ params['sns-project-id'] }}.{{ ti.xcom_pull(task_ids='set_xcom_values.get_accessible_by_olvin_dataset') }}.{{ params['visits_to_malls_daily_table'] }}"
        destination_dataset = "{{ dag_run.conf['dataset_postgres_template'] }}"
        destination_table = "{{ params['visits_to_malls_daily_table'] }}"

        destination = (
            f"{Variable.get('env_project')}.{destination_dataset}.{destination_table}"
        )

        staging = formatting.accessible_table_name_format(
            "olvin", table=destination_table, dataset=destination_dataset
        )

        copy_to_accessible = SNSBigQueryOperator(
            task_id="copy_to_accessible_by_olvin",
            query=queries.copy_table(source, staging),
        )

        copy_to_working = OlvinBigQueryOperator(
            task_id="copy_to_working_by_olvin",
            query=queries.copy_table(staging, destination),
        )

        drop_staging = SNSBigQueryOperator(
            task_id="delete_from_accessible_by_olvin", query=queries.drop_table(staging)
        )

        (
            create_visits_to_malls_daily_task
            >> copy_to_accessible
            >> copy_to_working
            >> drop_staging
        )

    create_visits_to_malls_monthly_task = OlvinBigQueryOperator(
        task_id="create_visits_to_malls_monthly_table",
        query="{% include './include/bigquery/scaling_to_all_US_malls/create_visits_to_malls_monthly.sql' %}",
    )

    group >> create_visits_to_malls_monthly_task

    return create_visits_to_malls_monthly_task
