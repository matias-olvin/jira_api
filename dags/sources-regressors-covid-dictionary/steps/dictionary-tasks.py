from airflow.models import TaskInstance
from common.operators.bigquery import OlvinBigQueryOperator


def register(start: TaskInstance) -> TaskInstance:
    """Register tasks on the dag.

    Args:
        start (airflow.models.TaskInstance): Task instance all tasks will
            registered downstream from.

    Returns:
        airflow.models.TaskInstance: The last task node in this section.
    """
    restrictions_dictionary_table = OlvinBigQueryOperator(
        task_id="restrictions-dictionary-table",
        query="{% include './include/bigquery/restrictions-dictionary.sql' %}",
    )
    dictionary_table = OlvinBigQueryOperator(
        task_id="dictionary-table",
        query="{% include './include/bigquery/dictionary.sql' %}",
    )
    dictionary_zipcodes_table = OlvinBigQueryOperator(
        task_id="dictionary_zipcodes_table",
        query="{% include './include/bigquery/dictionary-zipcode.sql' %}",
    )
    dictionary_test = OlvinBigQueryOperator(
        task_id="dictionary-test",
        query="{% include './include/bigquery/dictionary-test.sql' %}",
    )
    (
        start
        >> restrictions_dictionary_table
        >> dictionary_table
        >> dictionary_test
        >> dictionary_zipcodes_table
    )

    return dictionary_zipcodes_table
