from __future__ import annotations

from airflow.models import DAG, TaskInstance
from common.operators.bigquery import OlvinBigQueryOperator
from airflow.sensors.external_task import ExternalTaskSensor


def register(start: TaskInstance, dag: DAG) -> TaskInstance:

    create_SGPlaceHomeZipCodeYearly_table = OlvinBigQueryOperator(
        task_id="create_SGPlaceHomeZipCodeYearly_table",
        query="{% include './bigquery/update_sgplaceactivity_with_zipcode_yearly/create_SGPlaceHomeZipCodeYearly_table.sql' %}",
    )

    wait_for_create_postgres_table_formatted = ExternalTaskSensor(
        task_id="wait_for_create_postgres_table_formatted",
        external_dag_id=dag.params["dag-processes-monthly-update-visitor-destination"],
        external_task_id="create_postgres_table_formatted",
        dag=dag,
        poke_interval=60 * 5,
        execution_date_fn=lambda dt: dt,
        mode="reschedule",
        timeout=20 * 24 * 60 * 60,
    )

    update_columns_in_sgplaceactivity = OlvinBigQueryOperator(
        task_id="update_columns_in_sgplaceactivity",
        query="{% include './bigquery/update_sgplaceactivity_with_zipcode_yearly/update_columns_in_sgplaceactivity.sql' %}",
    )

    (
        start
        >> create_SGPlaceHomeZipCodeYearly_table
        >> wait_for_create_postgres_table_formatted
        >> update_columns_in_sgplaceactivity
    )

    return update_columns_in_sgplaceactivity
