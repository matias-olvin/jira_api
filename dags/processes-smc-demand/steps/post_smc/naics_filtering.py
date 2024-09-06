import pandas as pd
import pendulum
from airflow.models import DAG, TaskInstance, Variable
from airflow.utils.task_group import TaskGroup
from common.operators.bigquery import OlvinBigQueryOperator
from common.operators.exceptions import MarkSuccessOperator


def register(start: TaskInstance, dag: DAG) -> TaskGroup:
    

    with TaskGroup(group_id="post_smc_naics_filtering_task_group") as group:
        query_naics_filtering = OlvinBigQueryOperator(
            task_id="post_smc_query_naics_filtering",
            query="{% include './include/bigquery/post_smc/naics_filtering.sql' %}",
        )


        naics_filtering_end = MarkSuccessOperator(task_id="post_smc_check_naics_filtering_end",dag=dag)

        (
            start
            >> query_naics_filtering
            >> naics_filtering_end
        )

    return group
