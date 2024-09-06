from airflow.models import DAG, TaskInstance
from common.operators.bigquery import OlvinBigQueryOperator


def register(start: TaskInstance, dag: DAG, postgres_dataset: str) -> TaskInstance:

    update_sgcenters_column = OlvinBigQueryOperator(
        task_id=f"update_sgcenters_column_{postgres_dataset}",
        query='{% with postgres_dataset="'
        f"{postgres_dataset}"
        '"%}{% include "./bigquery/update_sgcenters/update_sgcenters_column.sql" %}{% endwith %}',
    )

    create_SGCenterCameoRaw_table = OlvinBigQueryOperator(
        task_id=f"create_SGCenterCameoRaw_table_{postgres_dataset}",
        query='{% with postgres_dataset="'
        f"{postgres_dataset}"
        '"%}{% include "./bigquery/update_sgcenters/create_SGCenterCameoRaw_table.sql" %}{% endwith %}',
        billing_tier="med",
    )

    create_SGCenterCameoMonthlyRaw_table = OlvinBigQueryOperator(
        task_id=f"create_SGCentersCameoMonthlyRaw_table_{postgres_dataset}",
        query='{% with postgres_dataset="'
        f"{postgres_dataset}"
        '"%}{% include "./bigquery/update_sgcenters/create_SGCenterCameoMonthlyRaw_table.sql" %}{% endwith %}',
        billing_tier="med",
    )

    (
        start
        >> update_sgcenters_column
        >> create_SGCenterCameoRaw_table
        >> create_SGCenterCameoMonthlyRaw_table
    )

    return create_SGCenterCameoMonthlyRaw_table
