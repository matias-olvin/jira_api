from airflow.models import DAG, TaskInstance
from common.operators.bigquery import OlvinBigQueryOperator


def register(start: TaskInstance, dag: DAG) -> TaskInstance:

    # s2_token_size gets injected into query_events_dictionary
    s2_token_size = 13

    query_events_dictionary = OlvinBigQueryOperator(
        task_id="query_events_dictionary",
        query='{% with s2_token_size="'
        f"{s2_token_size}"
        '"%}{% include "./include/bigquery/query_events_dictionary.sql" %}{% endwith %}',
    )

    dictionary_test = OlvinBigQueryOperator(
        task_id="dictionary-test",
        query="{% include './include/bigquery/dictionary-test.sql' %}",
    )

    start >> query_events_dictionary >> dictionary_test

    return dictionary_test
