"""
DAG ID: sg_networks_pipeline
"""
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyTableOperator,
    BigQueryDeleteTableOperator,
    BigQueryInsertJobOperator,
)
from airflow.sensors.external_task import ExternalTaskSensor


def register(dag, start):
    create_nodeid_table = BigQueryCreateEmptyTableOperator(
        task_id="create_nodeid_table",
        dataset_id="{{ params['networks_staging_dataset'] }}",
        table_id="{{ params['neo4j_id_table'] }}",
        schema_fields=[
            {"name": "ID", "type": "STRING", "mode": "REQUIRED"},
            {"name": "fk_sgplaces", "type": "STRING", "mode": "REQUIRED"},
        ],
        labels={
            "pipeline": "{{ dag.dag_id }}",
            "task_id": "{{ task.task_id.lower()[:63] }}",
        },
        dag=dag,
    )

    create_predicted_edges_table = BigQueryCreateEmptyTableOperator(
        task_id="create_predicted_table",
        dataset_id="{{ params['networks_staging_dataset'] }}",
        table_id="{{ params['predicted_edges_table'] }}",
        schema_fields=[
            {"name": "src_node", "type": "STRING", "mode": "REQUIRED"},
            {"name": "dst_node", "type": "STRING", "mode": "REQUIRED"},
            {"name": "weight", "type": "FLOAT", "mode": "REQUIRED"},
        ],
        labels={
            "pipeline": "{{ dag.dag_id }}",
            "task_id": "{{ task.task_id.lower()[:63] }}",
        },
        dag=dag,
    )

    export_date = "{{ execution_date.add(months=1).format('%Y%m01') }}"

    import_id = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="import_id",
        configuration={
            "load": {
                "sourceUris": (
                    (
                        f"https://storage.cloud.google.com/{dag.params['neo4j_bucket']}/{export_date}"
                        "/neo4j_id.csv"
                    )
                ),
                "sourceFormat": "CSV",
                "destinationTable": {
                    "projectId": "{{ params['project'] }}",  # Variable.get("storage_project_id"),
                    "datasetId": "{{ params['networks_staging_dataset'] }}",
                    "tableId": "{{ params['neo4j_id_table'] }}",
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_IF_NEEDED",
                "skipLeadingRows": 1,
            },
            "labels": {
                "pipeline": "{{ dag.dag_id }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
    )

    import_edges = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="import_edges",
        configuration={
            "load": {
                "sourceUris": (
                    (
                        f"https://storage.cloud.google.com/{dag.params['neo4j_bucket']}/{export_date}"
                        "/relationships_relation_predicted_*.csv"
                    )
                ),
                "sourceFormat": "CSV",
                "destinationTable": {
                    "projectId": "{{ params['project'] }}",  # Variable.get("storage_project_id"),
                    "datasetId": "{{ params['networks_staging_dataset'] }}",
                    "tableId": "{{ params['predicted_edges_table'] }}",
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_IF_NEEDED",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
    )

    mapping_id = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="mapping_id",
        configuration={
            "query": {
                "query": "{% include './bigquery/output_link_prediction/mapid.sql' %}",
                "useLegacySql": "False",
                "destinationTable": {
                    "projectId": "{{ params['project'] }}",  # Variable.get("storage_project_id"),
                    "datasetId": "{{ params['networks_staging_dataset'] }}",
                    "tableId": "{{ params['predicted_mapping_id_table'] }}",
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_IF_NEEDED",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )

    ## Deleting the same month
    delete_historical_mapping_id = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="delete_historical_mapping_id",
        configuration={
            "query": {
                "query": """DELETE `{{ params['project'] }}.{{ params['networks_dataset'] }}.{{ params['predicted_mapping_id_table'] }}` where local_date = '{{ execution_date.add(months=1).format('%Y-%m-01') }}'""",
                "useLegacySql": "False",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )

    ## Appending
    append_to_historical_mapping_id = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="append_to_historical_mapping_id",
        configuration={
            "query": {
                "query": """
                            select *,
                                    DATE("{{ execution_date.add(months=1).format('%Y-%m-01') }}") AS local_date
                            from `{{ params['project'] }}.{{ params['networks_staging_dataset'] }}.{{ params['predicted_mapping_id_table'] }}`
                        """,
                "useLegacySql": "False",
                "destinationTable": {
                    "projectId": "{{ params['project'] }}",
                    "datasetId": "{{ params['networks_dataset'] }}",
                    "tableId": "{{ params['predicted_mapping_id_table'] }}",
                },
                "timePartitioning": {"type": "DAY", "field": "local_date"},
                "clustering": {"fields": ["src_node", "dst_node"]},
                "writeDisposition": "WRITE_APPEND",
                "createDisposition": "CREATE_IF_NEEDED",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )

    """transform_k_edges = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="transform_k_edges",
        
        configuration={
            "query": {
                "query": "{% include './bigquery/output_link_prediction/transformation.sql' %}",
                "useLegacySql": "False",
                "destinationTable": {
                    "projectId": "{{ params['project'] }}",  # Variable.get("storage_project_id"),
                    "datasetId": "{{ params['networks_staging_dataset'] }}",
                    "tableId": "{{ params['estimated_connections_table'] }}",
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_IF_NEEDED",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id }}", 
                "task_id": "{{ task.task_id.lower()[:63] }}"
            }
        },
        dag=dag,
        location="EU",
    )"""

    ## Deleting the same month
    delete_monitoring_estimated = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="delete_monitoring_estimated",
        configuration={
            "query": {
                "query": """DELETE 
                `{{ params['project'] }}.{{ params['networks_metrics_dataset'] }}.{{ params['output_weight_vs_features_table'] }}` where local_date = '{{ execution_date.add(months=1).format('%Y-%m-01') }}'""",
                "useLegacySql": "False",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )

    ## Appending
    sampling_for_monitoring_estimated = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="sampling_for_monitoring_estimated",
        configuration={
            "query": {
                "query": """
                            select * except(random_nb)
                            from
                            (   
                            SELECT 
                                *,
                                row_number() over (order by rand()) / count(*) over() as random_nb
                            FROM 
                               `{{ params['project'] }}.{{ params['networks_staging_dataset'] }}.{{ params['predicted_mapping_id_table'] }}`
                            )
                            where random_nb < 0.1
                        """,
                "useLegacySql": "False",
                "destinationTable": {
                    "projectId": "{{ params['project'] }}",
                    "datasetId": "{{ params['networks_staging_dataset'] }}",
                    "tableId": "{{ params['estimated_connections_table_sample'] }}",
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_IF_NEEDED",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )

    ## Appending
    append_to_monitoring_estimated = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="append_to_monitoring_estimated",
        configuration={
            "query": {
                "query": (
                    '{% with connections_edges_table="'
                    f"{dag.params['estimated_connections_table_sample']}"
                    '"%}{% include "./bigquery/metrics/weight_vs_features.sql" %}{% endwith %}'
                ),
                "useLegacySql": "False",
                "destinationTable": {
                    "projectId": "{{ params['project'] }}",
                    "datasetId": "{{ params['networks_metrics_dataset'] }}",
                    "tableId": "{{ params['output_weight_vs_features_table'] }}",
                },
                "writeDisposition": "WRITE_APPEND",
                "createDisposition": "CREATE_IF_NEEDED",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )

    ## Deleting the same month
    delete_historical_transform_k_edges = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="delete_historical_transform_k_edges",
        configuration={
            "query": {
                "query": """DELETE `{{ params['project'] }}.{{ params['networks_dataset'] }}.{{ params['predicted_mapping_id_table'] }}` where local_date = DATE('{{ execution_date.add(months=1).format('%Y-%m-01') }}')""",
                "useLegacySql": "False",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )

    ## Appending
    append_to_final_k_edges = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="append_to_final_k_edges",
        configuration={
            "query": {
                "query": """
                            select *,
                                    DATE("{{ execution_date.add(months=1).format('%Y-%m-01') }}") AS local_date
                            from `{{ params['project'] }}.{{ params['networks_staging_dataset'] }}.{{ params['predicted_mapping_id_table'] }}`
                        """,
                "useLegacySql": "False",
                "destinationTable": {
                    "projectId": "{{ params['project'] }}",
                    "datasetId": "{{ params['networks_dataset'] }}",
                    "tableId": "{{ params['predicted_mapping_id_table'] }}",
                },
                "timePartitioning": {"type": "DAY", "field": "local_date"},
                "clustering": {"fields": ["src_node", "dst_node"]},
                "writeDisposition": "WRITE_APPEND",
                "createDisposition": "CREATE_IF_NEEDED",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )

    # Smooth resulting connections

    query_mixed_conn = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="query_mixed_conn",
        configuration={
            "query": {
                "query": "{% include './bigquery/weight_smoothing/0_mixed_conn.sql' %}",
                "useLegacySql": "False",
                "destinationTable": {
                    "projectId": "{{ params['project'] }}",
                    "datasetId": "{{ params['networks_dataset'] }}",
                    "tableId": "{{ params['mixed_conn_table'] }}",
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_IF_NEEDED",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )

    query_second_deg_conn = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="query_second_deg_conn",
        configuration={
            "query": {
                "query": "{% include './bigquery/weight_smoothing/11_second_deg.sql' %}",
                "useLegacySql": "False",
                "destinationTable": {
                    "projectId": "{{ params['project'] }}",
                    "datasetId": "{{ params['networks_staging_dataset'] }}",
                    "tableId": "{{ params['second_deg_conn_table'] }}",
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_IF_NEEDED",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )

    query_smooth_conn = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="query_smooth_conn",
        configuration={
            "query": {
                "query": "{% include './bigquery/weight_smoothing/12_smoothing.sql' %}",
                "useLegacySql": "False",
                "destinationTable": {
                    "projectId": "{{ params['project'] }}",
                    "datasetId": "{{ params['networks_dataset'] }}",
                    "tableId": "{{ params['smooth_conn_table'] }}",
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_IF_NEEDED",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )

    wait_for_activity = ExternalTaskSensor(
        task_id="wait_for_activity",
        external_dag_id="processes-monthly-update-agg-stats",
        external_task_id="propagate_activity_Place_Activity",
        dag=dag,
        poke_interval=60 * 20,
        execution_date_fn=lambda dt: dt,
        mode="reschedule",
        timeout=11 * 24 * 60 * 60,
    )
    start >> wait_for_activity

    query_postgres_table = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="query_postgres_table",
        configuration={
            "query": {
                "query": "{% include './bigquery/weight_smoothing/2_final_scaling_and_formatting.sql' %}",
                "useLegacySql": "False",
                "destinationTable": {
                    "projectId": "{{ params['project'] }}",
                    "datasetId": "{{ params['places_postgres_dataset'] }}",
                    "tableId": "{{ params['postgres_connection_table'] }}",
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_IF_NEEDED",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )
    wait_for_activity >> query_postgres_table

    delete_second_deg_conn = BigQueryDeleteTableOperator(
        task_id=f"delete_second_deg_conn",
        deletion_dataset_table="{{ params['project'] }}.{{ params['networks_staging_dataset'] }}."
        "{{ params['second_deg_conn_table'] }}",
        dag=dag,
    )

    delete_mixed_conn = BigQueryDeleteTableOperator(
        task_id=f"delete_mixed_conn",
        deletion_dataset_table="{{ params['project'] }}.{{ params['networks_dataset'] }}."
        "{{ params['mixed_conn_table'] }}",
        dag=dag,
    )

    '''
    
    create_postgres_table = BigQueryCreateEmptyTableOperator(
        
        task_id="create_postgres_table",
        dataset_id="{{ params['networks_staging_dataset'] }}",
        table_id="{{ params['postgres_connection_table'] }}",
        schema_fields=[
            {"name": "fk_sgplaces", "type": "STRING", "mode": "REQUIRED"},
            {"name": "connections", "type": "STRING", "mode": "REQUIRED"},
            {"name": "Date", "type": "DATE", "mode": "REQUIRED"},
        ],
        labels={
            "pipeline": "{{ dag.dag_id }}", 
            "task_id": "{{ task.task_id.lower()[:63] }}"
        },
        dag=dag,
    )

    postgres_json = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="postgres_json",
        
        configuration={
            "query": {
                "query": "{% include './bigquery/output_link_prediction/postgres_json.sql' %}",
                "useLegacySql": "False",
                "destinationTable": {
                    "projectId": "{{ params['project'] }}",  # Variable.get("storage_project_id"),
                    "datasetId": "{{ params['networks_staging_dataset'] }}",
                    "tableId": "{{ params['postgres_connection_table'] }}",
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_IF_NEEDED",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id }}", 
                "task_id": "{{ task.task_id.lower()[:63] }}"
            }
        },
        dag=dag,
        location="EU",
    )

    ## Deleting the same month
    delete_historical_postgres_json = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="delete_historical_postgres_json",
        
        configuration={
            "query": {
                "query": """DELETE `{{ params['project'] }}.{{ params['networks_dataset'] }}.
                {{ params['postgres_connection_table'] }}` where local_date = IF ('{{params['mode']}}' = 'historical', PARSE_DATE('%F', '{{ execution_date.add(months=1).format('%Y-%m-01') }}'), DATE("{{ execution_date.add(months=1).format('%Y-%m-01') }}") )""",
                "useLegacySql": "False",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id }}", 
                "task_id": "{{ task.task_id.lower()[:63] }}"
            }
        },
        dag=dag,
        location="EU",
    )

    ## Appending
    append_to_historical_postgres_json = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="append_to_historical_postgres_json",
        
        configuration={
            "query": {
                "query": """
                            select *,
                                    IF (
                                        '{{params['mode']}}' = 'historical',
                                        PARSE_DATE('%F', '{{ execution_date.add(months=1).format('%Y-%m-01') }}'),
                                        DATE("{{ execution_date.add(months=1).format('%Y-%m-01') }}")
                                        ) AS local_date
                            from `{{ params['project'] }}.{{ params['networks_staging_dataset'] }}.{{ params['postgres_connection_table'] }}`
                        """,
                "useLegacySql": "False",
                "destinationTable": {
                    "projectId": "{{ params['project'] }}",
                    "datasetId": "{{ params['networks_dataset'] }}",
                    "tableId": "{{ params['postgres_connection_table'] }}",
                },
                "timePartitioning": {"type": "DAY", "field": "local_date"},
                "clustering": {"fields": ["fk_sgplaces"]},
                "writeDisposition": "WRITE_APPEND",
                "createDisposition": "CREATE_IF_NEEDED",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id }}", 
                "task_id": "{{ task.task_id.lower()[:63] }}"
            }
        },
        dag=dag,
        location="EU",
    )'''

    output_end = DummyOperator(task_id="output_end", dag=dag)
    import_end = DummyOperator(task_id="import_end", dag=dag)

    start >> [create_nodeid_table, create_predicted_edges_table]
    create_nodeid_table >> import_id
    create_predicted_edges_table >> import_edges
    (
        [import_edges, import_id]
        >> import_end
        >> mapping_id
        >> delete_historical_transform_k_edges
        >> append_to_final_k_edges
    )

    (
        append_to_final_k_edges
        >> query_mixed_conn
        >> query_second_deg_conn
        >> query_smooth_conn
        >> query_postgres_table
        >> output_end
        >> [delete_second_deg_conn, delete_mixed_conn]
    )

    (
        query_smooth_conn
        >> delete_monitoring_estimated
        >> sampling_for_monitoring_estimated
        >> append_to_monitoring_estimated
    )
    (
        mapping_id
        >> delete_historical_mapping_id
        >> append_to_historical_mapping_id
        >> output_end
    )

    return output_end
