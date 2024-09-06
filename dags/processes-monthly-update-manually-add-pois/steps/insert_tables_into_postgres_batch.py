from airflow.models import DAG, TaskInstance
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from common.operators.bigquery import OlvinBigQueryOperator
from common.operators.exceptions import MarkSuccessOperator


def register(start: TaskInstance, dag: DAG) -> TaskGroup:
    with TaskGroup(group_id="insert_tables_into_postgres_batch_equivalents") as group:

        check_manual_pois_before_insert = MarkSuccessOperator(
            task_id="check_manual_pois_before_insert"
        )

        tables_to_insert = [
            "sgplace_activity_table",
            "sgplace_hourly_all_visits_raw_table",
            "sgplace_hourly_visits_raw_table",
            "sgplacedailyvisitsraw_table",
            "sgplace_monthly_visits_raw",
            "sgplacecameoraw_table",
            "sgplacecameomonthlyraw_table",
            "sgplaceraw_table",
            "sgplace_visitor_brand_destination_table",
            "sgplace_visitor_brand_destination_percentage_table",
        ]

        snapshot_tasks = list()
        insert_tasks = list()

        for table in tables_to_insert:

            rendered_table_name = dag.params[table]
            snapshot_task = OlvinBigQueryOperator(
                task_id=f"create_snapshot_of_postgres_batch_{rendered_table_name}",
                query='{% with table_name="'
                f"{rendered_table_name}"
                '"%}{% include "./include/bigquery/snapshots/create_snapshot_of_postgres_batch.sql" %}{% endwith %}',
            )

            snapshot_tasks.append(snapshot_task)

            insert_task = OlvinBigQueryOperator(
                task_id=f"insert_manual_pois_into_postgres_batch_{rendered_table_name}",
                query=f"{{% include './include/bigquery/insert_tables_into_postgres_batch/insert_into_{table}.sql' %}}",
                billing_tier="med",
            )

            insert_tasks.append(insert_task)

        insert_tasks_end = EmptyOperator(task_id="insert_tasks_end")

        snapshot_tasks_end = EmptyOperator(task_id="snapshot_tasks_end")

        (
            start
            >> check_manual_pois_before_insert
            >> snapshot_tasks
            >> snapshot_tasks_end
            >> insert_tasks
            >> insert_tasks_end
        )

    return group
