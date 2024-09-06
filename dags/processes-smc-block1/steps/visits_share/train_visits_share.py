from airflow.operators.bash_operator import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.hooks.compute_ssh import ComputeEngineSSHHook
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import TaskInstance, DAG, Variable
from common.utils import callbacks, slack_users
from common.operators.bigquery import OlvinBigQueryOperator


def register(start: TaskInstance, dag: DAG) -> TaskGroup:
    """Register tasks on the dag.

    Args:
        dag (airflow.models.DAG): DAG instance to register tasks on.
        start (airflow.models.TaskInstance): Task instance all tasks will
            registered downstream from.

    Returns:
        airflow.models.TaskInstance: The last task node in this section.
    """
    with TaskGroup(group_id="train_visits_share_task_group") as group:

        MACHINE_NAME = f"{dag.params['visits_share_vm']}"

        # Delete staging data
        delete_visits_share_data = GCSDeleteObjectsOperator(
            task_id="delete_visits_share_data",
            depends_on_past=False,
            dag=dag,
            bucket_name=dag.params["storage_bucket"],
            prefix="visits_share/{{ ds_nodash }}/*",
        )
        start >> delete_visits_share_data

        # Export to GCS
        export_visits_share_input = BashOperator(
            task_id="export_visits_share_input",
            bash_command="""
    gcloud config set project {{ var.value.storage_project_id }};
    bq extract --destination_format CSV "{{ params['smc_visits_share_dataset'] }}.{{ params['model_input_complete_table'] }}" "gs://{{ params['visits_share_bucket'] }}/{{ ds_nodash }}/classifier/model_input/*.csv";
    bq extract --destination_format CSV "{{ params['smc_visits_share_dataset'] }}.{{ params['prior_distances_table'] }}" "gs://{{ params['visits_share_bucket'] }}/{{ ds_nodash }}/prior_distance/model_input/*.csv"
    gcloud config set project {{ var.value.storage_project_id }};
            """,
            dag=dag,
        )
        delete_visits_share_data >> export_visits_share_input

        create_vm_visits_share = BashOperator(
            task_id="create_vm_visits_share",
            bash_command=f"gcloud beta compute instances create {MACHINE_NAME} "
            "--project {{ var.value.env_project }} "
            "--zone {{ params['notebook_location'] }} "
            "--source-machine-image {{ params['visits_share_vm_template'] }} "
            "--machine-type={{ params['visits_share_notebook_data_machine'] }}",
            dag=dag,
        )
        start >> create_vm_visits_share

        label_vm_visits_share = BashOperator(
            task_id="label_vm_visits_share",
            bash_command=f"gcloud compute instances add-labels {MACHINE_NAME} "
            "--project={{ var.value.env_project }} "
            "--zone={{ params['notebook_location'] }} "
            "--labels=pipeline={{ params['pipeline_label_value'] }}",
            dag=dag,
        )
        create_vm_visits_share >> label_vm_visits_share

        # noinspection PyTypeChecker
        load_scripts_visits_share = SSHOperator(
            task_id="load_scripts_visits_share",
            ssh_hook=ComputeEngineSSHHook(
                instance_name=MACHINE_NAME,
                zone=f"{ dag.params['notebook_location'] }",
                project_id=f"{ Variable.get('env_project') }",
                use_oslogin=False,
                use_iap_tunnel=False,
            ),
            command="bash '/home/jupyter/{{ params['base_script'] }}' '{{ params['scripts_bucket'] }}/visits_share' ",
            cmd_timeout=None,
            dag=dag,
            retries=2,
        )
        label_vm_visits_share >> load_scripts_visits_share

        # noinspection PyTypeChecker
        load_input_visits_share = SSHOperator(
            task_id="load_input_visits_share",
            ssh_hook=ComputeEngineSSHHook(
                instance_name=MACHINE_NAME,
                zone=f"{ dag.params['notebook_location'] }",
                project_id=f"{ Variable.get('env_project') }",
                use_oslogin=False,
                use_iap_tunnel=False,
            ),
            command="bash '/home/jupyter/vm_scripts/{{ params['load_input_visits_share_script'] }}' '{{ params['visits_share_bucket'] }}/{{ ds_nodash }}'",
            cmd_timeout=None,
            dag=dag,
        )
        [
            export_visits_share_input,
            load_scripts_visits_share,
        ] >> load_input_visits_share

        # noinspection PyTypeChecker
        train_visits_share = SSHOperator(
            task_id="train_visits_share",
            ssh_hook=ComputeEngineSSHHook(
                instance_name=MACHINE_NAME,
                zone=f"{ dag.params['notebook_location'] }",
                project_id=f"{ Variable.get('env_project') }",
                use_oslogin=False,
                use_iap_tunnel=False,
            ),
            command="""sudo tmux new -s train "bash '/home/jupyter/vm_scripts/{{ params['train_visits_share_script'] }}' '{{ params['visits_share_package_name'] }}' """
            """'{{ params['visits_share_classifier_module'] }}' """
            """'{{ params['visits_share_distance_module'] }}'" """,
            cmd_timeout=None,
            dag=dag,
        )
        load_input_visits_share >> train_visits_share

        # noinspection PyTypeChecker
        load_model_visits_share = SSHOperator(
            task_id="load_model_visits_share",
            ssh_hook=ComputeEngineSSHHook(
                instance_name=MACHINE_NAME,
                zone=f"{ dag.params['notebook_location'] }",
                project_id=f"{ Variable.get('env_project') }",
                use_oslogin=False,
                use_iap_tunnel=False,
            ),
            command="bash '/home/jupyter/vm_scripts/{{ params['load_model_visits_share_script'] }}' "
            "'{{ params['visits_share_bucket'] }}/{{ ds_nodash }}'",
            cmd_timeout=None,
            dag=dag,
        )
        train_visits_share >> load_model_visits_share

        query_load_visits_share_model = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="query_load_visits_share_model",
            query="{% include './include/bigquery/visits_share/load_model_visits_share.sql' %}",
        )

        load_model_visits_share >> query_load_visits_share_model

        load_prior_distance_parameters = OlvinBigQueryOperator(
            project_id="{{ var.value.dev_project_id }}",
            task_id="load_prior_distance_parameters",
            query="{% include './include/bigquery/visits_share/prior_distances/load_prior_distances_parameters.sql' %}"
        )

        load_model_visits_share >> load_prior_distance_parameters

        delete_vm_visits_share = BashOperator(
            task_id="delete_vm_visits_share",
            bash_command=f"gcloud compute instances delete {MACHINE_NAME} "
            "--project={{ var.value.env_project }} "
            "--zone {{ params['notebook_location'] }}",
            dag=dag,
        )
        load_model_visits_share >> delete_vm_visits_share

        visits_share_end = EmptyOperator(
            task_id="visits_share_end",
            on_success_callback=callbacks.task_end_custom_slack_alert(
                slack_users.MATIAS, slack_users.IGNACIO, msg_title="*Visits Share* model created."
            ),
        )
        [
            delete_vm_visits_share,
            query_load_visits_share_model,
            load_prior_distance_parameters,
        ] >> visits_share_end

    return group
