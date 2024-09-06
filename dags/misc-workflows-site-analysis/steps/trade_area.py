"""
DAG ID: site_selection
"""
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


def register(dag, start):
    """Register tasks on the dag.

    Args:
        dag (airflow.models.DAG): DAG instance to register tasks on.
        start (airflow.models.TaskInstance): Task instance all tasks will
            registered downstream from.

    Returns:
        airflow.models.TaskInstance: The last task node in this section.
    """
    final_tasks = []
    for area_type in ["home", "work"]:
        query_trade_area = BigQueryInsertJobOperator(
            task_id=f"query_trade_area_{area_type}",
            project_id=Variable.get("compute_project_id"),
            configuration={
                "query": {
                    "query": f"{{% with "
                    f"source='{area_type}'"
                    f"%}}{{% include './bigquery/trade_area.sql' %}}{{% endwith %}}",
                    "useLegacySql": "False",
                },
                "labels": {
                    "pipeline": "{{ dag.dag_id }}",
                    "task_id": "{{ task.task_id.lower()[:63]}}",
                },
            },
            dag=dag,
            location="EU",
        )
        start >> query_trade_area
        final_tasks.append(query_trade_area)

        query_trade_zipcodes = BigQueryInsertJobOperator(
            task_id=f"query_trade_zipcodes_{area_type}",
            project_id=Variable.get("compute_project_id"),
            configuration={
                "query": {
                    "query": f"{{% with "
                    f"source='{area_type}'"
                    f"%}}{{% include './bigquery/trade_zipcodes.sql' %}}{{% endwith %}}",
                    "useLegacySql": "False",
                },
                "labels": {
                    "pipeline": "{{ dag.dag_id }}",
                    "task_id": "{{ task.task_id.lower()[:63]}}",
                },
            },
            dag=dag,
            location="EU",
        )
        start >> query_trade_zipcodes
        final_tasks.append(query_trade_zipcodes)

        query_trade_cblocks = BigQueryInsertJobOperator(
            task_id=f"query_trade_cblocks_{area_type}",
            project_id=Variable.get("compute_project_id"),
            configuration={
                "query": {
                    "query": f"{{% with "
                    f"source='{area_type}'"
                    f"%}}{{% include './bigquery/trade_cblocks.sql' %}}{{% endwith %}}",
                    "useLegacySql": "False",
                },
                "labels": {
                    "pipeline": "{{ dag.dag_id }}",
                    "task_id": "{{ task.task_id.lower()[:63]}}",
                },
            },
            dag=dag,
            location="EU",
        )
        start >> query_trade_cblocks
        final_tasks.append(query_trade_cblocks)

        query_visitors_coordinates = BigQueryInsertJobOperator(
            task_id=f"query_visitors_coordinates_{area_type}",
            project_id=Variable.get("compute_project_id"),
            configuration={
                "query": {
                    "query": f"{{% with "
                    f"source='{area_type}'"
                    f"%}}{{% include './bigquery/visitors_homes_coordinates.sql' %}}{{% endwith %}}",
                    "useLegacySql": "False",
                },
                "labels": {
                    "pipeline": "{{ dag.dag_id }}",
                    "task_id": "{{ task.task_id.lower()[:63]}}",
                },
            },
            dag=dag,
            location="EU",
        )
        start >> query_visitors_coordinates
        final_tasks.append(query_visitors_coordinates)

        for level in range(5, 15):
            query_trade_cells = BigQueryInsertJobOperator(
                task_id=f"query_trade_cells_{area_type}_{level}",
                project_id=Variable.get("compute_project_id"),
                configuration={
                    "query": {
                        "query": f"{{% with "
                        f"source='{area_type}', "
                        f"level={level}"
                        f"%}}{{% include './bigquery/trade_cells.sql' %}}{{% endwith %}}",
                        "useLegacySql": "False",
                    },
                    "labels": {
                        "pipeline": "{{ dag.dag_id }}",
                        "task_id": "{{ task.task_id.lower()[:63]}}",
                    },
                },
                dag=dag,
                location="EU",
            )
            start >> query_trade_cells
            final_tasks.append(query_trade_cells)

    end = DummyOperator(task_id="end_trade_area", dag=dag)
    final_tasks >> end
    return end
