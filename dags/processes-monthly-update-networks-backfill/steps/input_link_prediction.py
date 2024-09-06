"""
DAG ID: sg_networks_pipeline
"""
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


def create_header(dag, **context):
    from google.cloud import bigquery, storage

    bq_client = bigquery.Client()
    storage_client = storage.Client()
    query_job = bq_client.query(
        f"""
            SELECT                          
            column_name,data_type
            FROM
            `{dag.params['project']}.{dag.params['networks_dataset']}.INFORMATION_SCHEMA.COLUMNS`
            WHERE
            table_name="{dag.params['node_features_table']}"
        """,
        job_config=bigquery.QueryJobConfig(
            labels={
                "pipeline": f"{dag.dag_id}",
                "task_id": f"{context['task'].task_id[:63]}",
            }
        ),
    )
    results = query_job.result()
    column_names_types = []
    map_types = {
        "STRING": "string",
        "FLOAT64": "double",
        "INT64": "int",
        "BOOLEAN": "int",
        "DATE": "string",
    }
    node_features_header = []
    connections_edges_header = [
        ":START_ID",
        "weight:float",
        ":END_ID",
        ":TYPE",
        "local_date:string",
    ]

    for result in results:
        column_names_types.append((result[0], result[1]))

    node_features_header.append("fk_sgplaces:ID")
    for column in column_names_types:
        if column[0] == "node_id":
            continue
        else:
            node_features_header.append(column[0] + ":" + map_types[column[1]])

    execution_date = context["execution_date"].date().strftime("%Y%m01")
    sep_symbol = ","
    destination_bucket = f'{dag.params["neo4j_bucket"]}'
    destination_file = f"{execution_date}/observed_connections_edges_header.csv"
    blob = storage_client.bucket(destination_bucket).blob(destination_file)
    connections_edges_header = sep_symbol.join(connections_edges_header)
    blob.upload_from_string(
        connections_edges_header, timeout=300.0, content_type="text/csv"
    )

    destination_file = f"{execution_date}/node_features_header.csv"
    blob = storage_client.bucket(destination_bucket).blob(destination_file)
    node_features_header = sep_symbol.join(node_features_header)
    blob.upload_from_string(
        node_features_header, timeout=300.0, content_type="text/csv"
    )

    # with open('gs://neo4j-data-prod-olvin-com/{{ ds.format('%Y-%m-01') }}/observed_connections_edges_header.csv', 'w', newline='') as csvfile:

    #     csvwriter = csv.writer(csvfile, delimiter=',',
    #                         quotechar='|', quoting=csv.QUOTE_MINIMAL)
    #     csvwriter.writerow(connections_edges_header)

    # with open('gs://neo4j-data-prod-olvin-com/{{ ds.format('%Y-%m-01') }}/node_features_header.csv', 'w', newline='') as csvfile:
    #     csvwriter = csv.writer(csvfile, delimiter=',',
    #                         quotechar='|', quoting=csv.QUOTE_MINIMAL)
    #     csvwriter.writerow(node_features_header)

    return


def register(dag, start):
    """Register tasks on the dag.
    Args:
        dag (airflow.models.DAG): DAG instance to register tasks on.
        start (airflow.models.TaskInstance): Task instance all tasks will
            registered downstream from.
    Returns:
        airflow.models.TaskInstance: The last task node in this section.
    """

    # Deleting the same month
    delete_historical_full_visits = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="delete_historical_full_visits",
        configuration={
            "query": {
                "query": """DELETE 
                `{{ params['project'] }}.{{ params['networks_staging_dataset'] }}.{{ params['full_visits_table'] }}` where local_date = '{{ ds.format('%Y-%m-01') }}'""",
                "useLegacySql": "False",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        on_failure_callback=None,
        dag=dag,
        location="EU",
    )

    query_full_visits = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="query_full_visits",
        configuration={
            "query": {
                "query": (
                    '{% with is_sample="'
                    f"{dag.params['is_sample']}"
                    '"%}{% include "./bigquery/input_link_prediction/full_visits.sql" %}{% endwith %}'
                ),
                "useLegacySql": "False",
                "destinationTable": {
                    "projectId": "{{ params['project'] }}",  # Variable.get("storage_project_id"),
                    "datasetId": "{{ params['networks_staging_dataset'] }}",
                    "tableId": "{{ params['full_visits_table'] }}",
                },
                "writeDisposition": "WRITE_APPEND",
                "createDisposition": "CREATE_IF_NEEDED",
                "timePartitioning": {"type": "MONTH", "field": "local_date"},
            },
            "labels": {
                "pipeline": "{{ dag.dag_id }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )
    start >> delete_historical_full_visits >> query_full_visits

    # Deleting the same month
    delete_connection_combinations = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="delete_connection_combinations",
        configuration={
            "query": {
                "query": """DELETE 
                    `{{ params['project'] }}.{{ params['networks_staging_dataset'] }}.{{ params['all_combinations_table'] }}`
                     where local_date = '{{ ds.format('%Y-%m-01') }}'""",
                "useLegacySql": "False",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        on_failure_callback=None,
        dag=dag,
        location="EU",
    )

    query_connection_combinations = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="query_connection_combinations",
        configuration={
            "query": {
                "query": "{% include './bigquery/input_link_prediction/connection_combinations.sql' %}",
                "useLegacySql": "False",
                "destinationTable": {
                    "projectId": "{{ params['project'] }}",  # Variable.get("storage_project_id"),
                    "datasetId": "{{ params['networks_staging_dataset'] }}",
                    "tableId": "{{ params['all_combinations_table'] }}",
                },
                "writeDisposition": "WRITE_APPEND",
                "createDisposition": "CREATE_IF_NEEDED",
                "timePartitioning": {"type": "MONTH", "field": "local_date"},
            },
            "labels": {
                "pipeline": "{{ dag.dag_id }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )
    query_full_visits >> delete_connection_combinations >> query_connection_combinations

    # Deleting the same month
    delete_connections_edges = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="delete_connections_edges",
        configuration={
            "query": {
                "query": """DELETE 
                    `{{ params['project'] }}.{{ params['networks_dataset'] }}.{{ params['observed_connections_edges_table'] }}`
                     where local_date = '{{ ds.format('%Y-%m-01') }}'""",
                "useLegacySql": "False",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        on_failure_callback=None,
        dag=dag,
        location="EU",
    )

    query_connections_edges = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="query_connections_edges",
        configuration={
            "query": {
                "query": "{% include './bigquery/input_link_prediction/connections_edges.sql' %}",
                "useLegacySql": "False",
                "destinationTable": {
                    "projectId": "{{ params['project'] }}",  # Variable.get("storage_project_id"),
                    "datasetId": "{{ params['networks_dataset'] }}",
                    "tableId": "{{ params['observed_connections_edges_table'] }}",
                },
                "writeDisposition": "WRITE_APPEND",
                "createDisposition": "CREATE_IF_NEEDED",
                "timePartitioning": {"type": "MONTH", "field": "local_date"},
            },
            "labels": {
                "pipeline": "{{ dag.dag_id }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )

    query_connection_combinations >> delete_connections_edges >> query_connections_edges

    # ## VISUALISATIONS
    # ## Deleting the same month
    # delete_historical_input_weight_vs_features = BigQueryInsertJobOperator(
    #     task_id="delete_historical_input_weight_vs_features",
    #     project_id="{{ params['project'] }}",
    #     configuration={
    #         "query": {
    #             "query": """DELETE `{{ params['project'] }}.{{ params['networks_metrics_dataset'] }}.{{ params['input_weight_vs_features_table'] }}` where local_date = '{{ ds.format('%Y-%m-01') }}'""",
    #             "useLegacySql": "False",
    #         },
    #         "labels": {
    #             "pipeline": "{{ dag.dag_id }}",
    # "task_id": "{{ task.task_id.lower()[:63] }}"
    #         }
    #     },
    #     dag=dag,
    #     location="EU",
    # )
    #
    # ## Appending
    # append_to_historical_input_weight_vs_features = BigQueryInsertJobOperator(
    #     task_id="append_to_historical_input_weight_vs_features",
    #     project_id="{{ params['project'] }}",
    #     configuration={
    #         "query": {
    #             "query": (
    #                 '{% with connections_edges_table="'
    #                 f"{dag.params['observed_connections_folder']}"
    #                 '"%}{% include "./bigquery/metrics/weight_vs_features.sql" %}{% endwith %}'
    #             ),
    #             "useLegacySql": "False",
    #             "destinationTable": {
    #                 "projectId": "{{ params['project'] }}",
    #                 "datasetId": "{{ params['networks_metrics_dataset'] }}",
    #                 "tableId": "{{ params['input_weight_vs_features_table'] }}",
    #             },
    #             "writeDisposition": "WRITE_APPEND",
    #             "createDisposition": "CREATE_IF_NEEDED",
    #         },
    #         "labels": {
    #             "pipeline": "{{ dag.dag_id }}",
    # "task_id": "{{ task.task_id.lower()[:63] }}"
    #         }
    #     },
    #     dag=dag,
    #     location="EU",
    # )

    # We are not implementing the journey for this dag but in future this is the code to be used
    # query_journeys_edges = BigQueryInsertJobOperator(
    #     task_id="query_journeys_edges",
    #     project_id="{{ params['project'] }}",
    #     configuration={
    #         "query": {
    #             "query": "{% include './bigquery/input_link_prediction/journeys_edges.sql' %}",
    #             "useLegacySql": "False",
    #             "destinationTable": {
    #                 "projectId": "{{ params['project'] }}",#Variable.get("storage_project_id"),
    #                 "datasetId": "{{ params['networks_staging_dataset'] }}",
    #                 "tableId": "{{ params['journeys_edges_table'] }}",
    #             },
    #             "writeDisposition": "WRITE_TRUNCATE",
    #             "createDisposition": "CREATE_IF_NEEDED",
    #         },
    #        "labels": {
    #            "pipeline": "{{ dag.dag_id }}",
    # "task_id": "{{ task.task_id.lower()[:63] }}"
    #        }
    #     },
    #     dag=dag,
    #     location="EU",
    # )

    # We are only using the historical node features table at the moment so we do not calculate the node features using the
    # node_features.sql
    # query_node_features = BigQueryInsertJobOperator(
    #     task_id="query_node_features",
    #     project_id="{{ params['project'] }}",
    #     configuration={
    #         "query": {
    #             "query": "{% include './bigquery/input_link_prediction/node_features.sql' %}",
    #             "useLegacySql": "False",
    #             "destinationTable": {
    #                 "projectId": "{{ params['project'] }}",#Variable.get("storage_project_id"),
    #                 "datasetId": "{{ params['networks_staging_dataset'] }}",
    #                 "tableId": "{{ params['node_features_table_intermediate'] }}",
    #             },
    #             "writeDisposition": "WRITE_TRUNCATE",
    #             "createDisposition": "CREATE_IF_NEEDED",
    #         },
    #        "labels": {
    #            "pipeline": "{{ dag.dag_id }}",
    # "task_id": "{{ task.task_id.lower()[:63] }}"
    #        }
    #     },
    #     dag=dag,
    #     location="EU",
    # )
    # (f"gs://neo4j_demo/ {{ execution_date.strftime('%Y%m%d') }} /historical_edges_*.csv.gzip")

    # IN THE FUTURE, THIS WILL READ FROM POI REPRESENTATION
    load_poi_representation = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="load_poi_representation",
        configuration={
            "query": {
                "query": "{% include './bigquery/input_link_prediction/loading_poi_representation.sql' %}",
                "useLegacySql": "False",
                "destinationTable": {
                    "projectId": "{{ params['project'] }}",  # Variable.get("storage_project_id"),
                    "datasetId": "{{ params['networks_staging_dataset'] }}",
                    "tableId": "{{ params['node_features_raw_table'] }}",
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

    feature_processing = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="feature_processing",
        configuration={
            "query": {
                "query": "{% include './bigquery/input_link_prediction/process_nodes_features.sql' %}",
                "useLegacySql": "False",
                "destinationTable": {
                    "projectId": "{{ params['project'] }}",  # Variable.get("storage_project_id"),
                    "datasetId": "{{ params['networks_staging_dataset'] }}",
                    "tableId": "{{ params['node_features_all_table'] }}",
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

    select_nodes_and_features = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="select_nodes_and_features",
        configuration={
            "query": {
                "query": (
                    '{% with is_sample="'
                    f"{dag.params['is_sample']}"
                    '"%}{% include "./bigquery/input_link_prediction/select_nodes_and_features.sql" %}{% endwith %}'
                ),
                "useLegacySql": "False",
                "destinationTable": {
                    "projectId": "{{ params['project'] }}",  # Variable.get("storage_project_id"),
                    "datasetId": "{{ params['networks_dataset'] }}",
                    "tableId": "{{ params['node_features_table'] }}",
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

    create_header_task = PythonOperator(
        task_id="create_header_task",
        python_callable=create_header,
        op_kwargs={
            "dag": dag,
        },
        templates_dict={
            "pipeline": "{{ dag.dag_id }}",
            "task_id": "{{ task.task_id.lower()[:63] }}",
        },
        provide_context=True,
        dag=dag,
    )

    export_date = "{{ ds_nodash.format('%Y%m01') }}"

    export_nodes = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="export_nodes",
        configuration={
            "extract": {
                "destinationUri": (
                    (
                        f"gs://{dag.params['neo4j_bucket']}/{export_date}/{dag.params['nodes_folder']}_*.csv.gzip"
                    )
                ),
                "destinationFormat": "CSV",
                "compression": "GZIP",
                "printHeader": False,
                "sourceTable": {
                    "projectId": "{{ params['project']}}",
                    "datasetId": "{{ params['networks_dataset'] }}",
                    "tableId": "{{ params['node_features_table'] }}",
                },
            },
            "labels": {
                "pipeline": "{{ dag.dag_id }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
    )

    export_connection_edges = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="export_connection_edges",
        configuration={
            "extract": {
                "destinationUri": (
                    (
                        f"gs://{dag.params['neo4j_bucket']}/{export_date}"
                        f"/{dag.params['observed_connections_folder']}_*.csv.gzip"
                    )
                ),
                "destinationFormat": "CSV",
                "compression": "GZIP",
                "printHeader": False,
                "sourceTable": {
                    "projectId": "{{ params['project'] }}",
                    "datasetId": "{{ params['networks_dataset'] }}",
                    "tableId": "{{ params['observed_connections_edges_table'] }}${{ execution_date.strftime('%Y%m') }}",
                },
            },
            "labels": {
                "pipeline": "{{ dag.dag_id }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
    )

    input_link_prediction_end = DummyOperator(
        task_id="input_link_prediction_end", dag=dag
    )

    # For generating the edges for the neo4j graph

    start >> load_poi_representation

    query_connections_edges >> create_header_task

    (
        load_poi_representation
        >> feature_processing
        >> select_nodes_and_features
        >> create_header_task
    )

    # query_connections_edges >> delete_historical_input_weight_vs_features >> append_to_historical_input_weight_vs_features
    # query_connections_edges >> delete_historical_observed_connections >> append_to_historical_observed_connections

    (
        create_header_task
        >> [
            export_connection_edges,
            export_nodes,
        ]
        >> input_link_prediction_end
    )

    return input_link_prediction_end
