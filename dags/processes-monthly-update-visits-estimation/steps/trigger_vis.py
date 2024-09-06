from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


def register(dag, start):
    delete_time_series_poi_visualisations = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="delete_time_series_poi_visualisations",
        configuration={
            "query": {
                "query": f"""DELETE `{{{{ var.value.env_project }}}}.{{{{ params['metrics_dataset'] }}}}.{{{{ params['time_series_visualisations_poi_table'] }}}}` where run_date = '{{{{ ds }}}}' AND step = '{{{{ dag_run.conf['step'] }}}}' AND mode = 'all'""",
                "useLegacySql": "False",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id |  replace('.','-') }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )

    time_series_poi_visualisations = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id="time_series_poi_visualisations",
        configuration={
            "query": {
                "query": "{% include './bigquery/time_series_vis_poi.sql' %}",
                "useLegacySql": "False",
                "destinationTable": {
                    "projectId": "{{ var.value.env_project }}",
                    "datasetId": "{{ params['metrics_dataset'] }}",
                    "tableId": "{{ params['time_series_visualisations_poi_table'] }}",
                },
                "writeDisposition": "WRITE_APPEND",
                "createDisposition": "CREATE_IF_NEEDED",
            },
            "labels": {
                "pipeline": "{{ dag.dag_id |  replace('.','-') }}",
                "task_id": "{{ task.task_id.lower()[:63] }}",
            },
        },
        dag=dag,
        location="EU",
    )

    start >> delete_time_series_poi_visualisations >> time_series_poi_visualisations

    return time_series_poi_visualisations
