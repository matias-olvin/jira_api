from airflow.models import DAG, TaskInstance
from common.operators.bigquery import OlvinBigQueryOperator
from common.utils.callbacks import task_fail_slack_alert


def register(start: TaskInstance, dag: DAG, env: str) -> TaskInstance:

    create_weather_dictionary = OlvinBigQueryOperator(
        task_id="create_weather_dictionary",
        query="{% include './include/bigquery/create_dictionary.sql' %}",
    )

    dictionary_test = OlvinBigQueryOperator(
        task_id="dictionary-test",
        query="{% include './include/bigquery/dictionary-test.sql' %}",
        on_failure_callback=task_fail_slack_alert("U02KJ556S1H", channel=env),
    )

    start >> create_weather_dictionary >> dictionary_test

    return dictionary_test
