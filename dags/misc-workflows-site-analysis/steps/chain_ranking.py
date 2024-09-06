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
    # Visits historical monthly visits
    query_chain_ranking_local = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="query_chain_ranking_local",
        project_id=Variable.get("compute_project_id"),
        configuration={
            "query": {
                "query": "{% include './bigquery/chain_ranking_local.sql' %}",
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
    query_chain_ranking_national = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="query_chain_ranking_national",
        project_id=Variable.get("compute_project_id"),
        configuration={
            "query": {
                "query": "{% include './bigquery/chain_ranking_national.sql' %}",
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
    query_chain_ranking_state = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="query_chain_ranking_state",
        project_id=Variable.get("compute_project_id"),
        configuration={
            "query": {
                "query": "{% include './bigquery/chain_ranking_state.sql' %}",
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
    query_visits_per_sq_ft_state = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="query_visits_per_sq_ft_state",
        project_id=Variable.get("compute_project_id"),
        configuration={
            "query": {
                "query": "{% include './bigquery/visits_per_sq_ft_state.sql' %}",
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

    query_visits_per_sq_ft_local = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="query_visits_per_sq_ft_local",
        project_id=Variable.get("compute_project_id"),
        configuration={
            "query": {
                "query": "{% include './bigquery/visits_per_sq_ft_local.sql' %}",
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
    query_visits_per_sq_ft_national = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="query_visits_per_sq_ft_national",
        project_id=Variable.get("compute_project_id"),
        configuration={
            "query": {
                "query": "{% include './bigquery/visits_per_sq_ft_national.sql' %}",
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
    chain_ranking_end = DummyOperator(task_id="chain_ranking_end", dag=dag)

    (
        start
        >> query_chain_ranking_local
        >> query_visits_per_sq_ft_local
        >> chain_ranking_end
    )
    (
        start
        >> query_chain_ranking_national
        >> query_visits_per_sq_ft_national
        >> chain_ranking_end
    )
    (
        start
        >> query_chain_ranking_state
        >> query_visits_per_sq_ft_state
        >> chain_ranking_end
    )

    return chain_ranking_end
