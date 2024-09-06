"""
DAG ID: visits_estimation
"""
from datetime import datetime

from airflow import AirflowException

# airflow imports
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
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

    def task_to_fail():
        """
        Task that will fail.
        """
        raise AirflowException("This task must be manually set to success to continue.")

    trigger_regressors_group_visits_dictionary = BashOperator(
        task_id="trigger_regressors_group_visits_dictionary",
        bash_command=f"gcloud composer environments run prod-sensormatic "
        f"--project {dag.params['sns_project']} "
        "--location europe-west1 "
        f"--impersonate-service-account {dag.params['cross_project_service_account']} "
        f'dags trigger -- -e "{{{{ ds }}}} {datetime.now().time()}" smc_time_factors_group_residuals_dictionary '
        "|| true ",  # this line is needed due to gcloud bug.
        dag=dag,
    )

    mark_success_to_complete_regressors = PythonOperator(
        task_id=f"mark_success_to_complete_regressors",
        python_callable=task_to_fail,
        dag=dag,
    )

    copy_task = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id=f"copy_group_visits",
        configuration={
            "query": {
                "query": f"{{% include './bigquery/copy_group_visits.sql' %}}",
                "useLegacySql": "false",
            }
        },
    )

    # # No Query for Zipcodes
    # dictionary_zipcode_table = BigQueryInsertJobOperator(
    #     task_id="dictionary_zipcode_table",
    #     # project_id=Variable.get("compute_project_id"),
    #     configuration={
    #         "query": {
    #             "query": "{% include './bigquery/dictionary_zipcode.sql' %}",
    #             "useLegacySql": "False",
    #             "destinationTable": {
    #                 "projectId": "{{ params['storage_project'] }}",
    #                 "datasetId": "{{ params['regressors_dataset'] }}",
    #                 "tableId": "{{ params['holidays_dictionary_zipcode_table'] }}",
    #             },
    #             "writeDisposition": "WRITE_TRUNCATE",
    #             "createDisposition": "CREATE_IF_NEEDED",
    #         }
    #     },
    #     dag=dag,
    #     location="EU",
    # )

    (
        start
        >> trigger_regressors_group_visits_dictionary
        >> mark_success_to_complete_regressors
        >> copy_task
    )  # >> dictionary_zipcode_table

    return copy_task
    # return dictionary_zipcode_table
