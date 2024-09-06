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
    query_states_demographics = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="query_states_demographics",
        project_id=Variable.get("compute_project_id"),
        configuration={
            "query": {
                "query": "{% include './bigquery/demographics_states.sql' %}",
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
    start >> query_states_demographics

    query_visitors_demographics = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="query_visitors_demographics",
        project_id=Variable.get("compute_project_id"),
        configuration={
            "query": {
                "query": "{% include './bigquery/visitors_demographics.sql' %}",
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
    query_states_demographics >> query_visitors_demographics

    query_visitors_demographics_display = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="query_visitors_demographics_display",
        project_id=Variable.get("compute_project_id"),
        configuration={
            "query": {
                "query": "{% include './bigquery/demographics_display.sql' %}",
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
    query_visitors_demographics >> query_visitors_demographics_display

    stats_groups = [
        "race",
        "age",
        "income",
        "delta_race",
        "delta_age",
        "delta_income",
        "education",
        "delta_education",
        "life_stage",
        "delta_life_stage",
        "house_inc_per",
        "prop_val",
    ]
    final_tasks = []
    for stats_group in stats_groups:
        query_demographics_export = BigQueryInsertJobOperator(
            task_id=f"query_demographics_export_{stats_group}",
            project_id=Variable.get("compute_project_id"),
            configuration={
                "query": {
                    "query": f"{{% with "
                    f"stats_group='{stats_group}'"
                    f"%}}{{% include './bigquery/demographics_export.sql' %}}{{% endwith %}}",
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
        query_visitors_demographics_display >> query_demographics_export
        final_tasks.append(query_demographics_export)

        query_median_stats = BigQueryInsertJobOperator(
            task_id=f"query_median_stats_{stats_group}",
            project_id=Variable.get("compute_project_id"),
            configuration={
                "query": {
                    "query": f"{{% with "
                    f"stats_group='{stats_group}'"
                    f"%}}{{% include './bigquery/median_stats.sql' %}}{{% endwith %}}",
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
        query_visitors_demographics_display >> query_median_stats
        final_tasks.append(query_median_stats)

    end = DummyOperator(task_id="end_demographics", dag=dag)
    final_tasks >> end
    return end
