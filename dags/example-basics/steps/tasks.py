from airflow.models import DAG, TaskInstance

from common.operators.bigquery import OlvinBigQueryOperator


def register(start: TaskInstance, dag: DAG) -> TaskInstance:
    example_bigquery_task_1 = OlvinBigQueryOperator(
        task_id="example_bigquery_task_1",
        query="{% include './include/bigquery/example_query_1.sql' %}"
    )
    example_bigquery_task_2 = OlvinBigQueryOperator(
        task_id="example_bigquery_task_2",
        query="{% include './include/bigquery/example_query_2.sql' %}"
    )
    example_bigquery_task_3 = OlvinBigQueryOperator(
        task_id="example_bigquery_task_3",
        query="{% include './include/bigquery/example_query_3.sql' %}"
    )
    example_bigquery_task_4 = OlvinBigQueryOperator(
        task_id="example_bigquery_task_4",
        query="{% include './include/bigquery/example_query_4.sql' %}"
    )

    start >> example_bigquery_task_1
    example_bigquery_task_1 >> example_bigquery_task_2
    example_bigquery_task_2 >> example_bigquery_task_3
    example_bigquery_task_3 >> example_bigquery_task_4

    return example_bigquery_task_4