from google.cloud.bigquery import Client
from common.operators.bigquery import OlvinBigQueryOperator
from airflow.models import TaskInstance, DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator


def register(start: TaskInstance, dag: DAG) -> TaskInstance:
    
    def get_fk_sgplaces_to_convert():

        client = Client()

        sql_query = f"""
        SELECT fk_sgplaces
        FROM `storage-prod-olvin-com.accessible-by-sns.SGPlaceHourlyVisitsRaw_bf`
        LIMIT 1;
        """

        query_job = client.query(sql_query)

        for row in query_job:
            value_to_check = row["fk_sgplaces"]

        return value_to_check

    def choose_branch():

        value_to_check = get_fk_sgplaces_to_convert()

        if "@" in value_to_check:
            return "convert_ids"
        else:
            return "conversion_not_needed"
        

    load_table = OlvinBigQueryOperator(
        task_id="load_accessible-by-sns_SGPlaceHourlyVisitsRaw_bf_from_gcs",
        query="{% include './include/load_query.sql' %}",
    )


    decision_end_task = EmptyOperator(task_id="decision_end", trigger_rule="one_success")

    deciding_task = BranchPythonOperator(
        task_id="check_for_olvin_id_conversion",
        python_callable=choose_branch,
        dag=dag,
    )

    conversion_not_needed_task = EmptyOperator(task_id="conversion_not_needed")

    convert_ids_task = OlvinBigQueryOperator(
    task_id="convert_ids",
    query="{% include './include/convert_ids.sql' %}",
    )

    start >> load_table >> deciding_task

    deciding_task >> conversion_not_needed_task
    deciding_task >> convert_ids_task

    (convert_ids_task, conversion_not_needed_task) >> decision_end_task
    
    return decision_end_task
