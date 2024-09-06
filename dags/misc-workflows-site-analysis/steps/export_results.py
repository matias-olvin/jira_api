"""
DAG ID: site_selection
"""
from airflow.models import Variable
from airflow.operators.python import PythonOperator
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

    def export_table(file_name):
        export_job = BigQueryInsertJobOperator(
            task_id=f"""export_{file_name.replace("{{ params['", "").replace("'] }}", "")}""",
            project_id=Variable.get("compute_project_id"),
            configuration={
                "extract": {
                    "destinationUri": (
                        (
                            f"gs://{{{{ params['site_selection_bucket'] }}}}/{{{{ dag_run.conf['site_name'] }}}}/{file_name}.csv"
                        )
                    ),
                    "printHeader": True,
                    "destinationFormat": "CSV",
                    "sourceTable": {
                        "projectId": Variable.get("storage_project_id"),
                        "datasetId": "{{ dag_run.conf['site_name'] }}",
                        "tableId": f"{file_name}",
                    },
                },
                "labels": {
                    "pipeline": "{{ dag.dag_id }}",
                    "task_id": "{{ task.task_id.lower()[:63]}}",
                },
            },
            dag=dag,
        )
        start >> export_job
        final_tasks.append(export_job)

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
    for stats_group in stats_groups:
        export_table(stats_group)
        export_table(f"{{{{ params['median_table'] }}}}_{stats_group}")

    for area_type in ["home", "work"]:
        export_table(f"{{{{ params['trade_area_table'] }}}}_{area_type}")
        export_table(f"{{{{ params['trade_zipcodes_table'] }}}}_{area_type}")
        export_table(f"{{{{ params['coordinates_table'] }}}}_{area_type}")
        for level in range(5, 15):
            export_table(f"{{{{ params['trade_cells_table'] }}}}_{area_type}_{level}")
    export_table("{{ params['visitors_homes_table'] }}")

    visits_tables = [
        "monthly_visits",
        "chain_ranking_national",
        "chain_ranking_state",
        "chain_ranking_local",
        "psychographics_brand",
        "psychographics_category",
        "void_brand",
        "void_category",
        "visits_per_sq_ft_state",
        "visits_per_sq_ft_national",
        "visits_per_sq_ft_local",
    ]

    for visits_table in visits_tables:
        export_table(visits_table)

    def submit_alert_user(**context):
        from airflow.hooks.base_hook import BaseHook
        from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

        user_msg = f"""*Site Analysis {context['templates_dict']['site_name']}* finished.
Results available at: https://console.cloud.google.com/storage/browser/{dag.params['site_selection_bucket']}/{context['templates_dict']['site_name']}
"""
        for user_id in ["U0180QS8MFV", "U7NUR99LY"]:
            user_msg = user_msg + f" <@{user_id}>"
        slack_webhook_token = BaseHook.get_connection("slack_notifications").password
        slack_msg = """
:bulb: {user_msg}
*Dag*: {dag}
                """.format(
            user_msg=user_msg,
            dag=context.get("task_instance").dag_id,
        )
        submit_alert = SlackWebhookOperator(
            task_id="slack_test",
            http_conn_id="slack_notifications",
            webhook_token=slack_webhook_token,
            message=slack_msg,
            username="airflow",
        )
        return submit_alert.execute(context=context)

    notify_export = PythonOperator(
        task_id="notify_export",
        python_callable=submit_alert_user,
        templates_dict={
            "site_name": "{{ dag_run.conf['site_name'] }}",
        },
        provide_context=True,
        dag=dag,
    )
    final_tasks >> notify_export
    return notify_export
