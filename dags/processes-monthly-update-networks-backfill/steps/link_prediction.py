"""
DAG ID: sg_networks_pipeline
"""
from datetime import timedelta

from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule


def register(dag, start):
    """Register tasks on the dag.
    Args:
        dag (airflow.models.DAG): DAG instance to register tasks on.
        start (airflow.models.TaskInstance): Task instance all tasks will
            registered downstream from.
    Returns:
        airflow.models.TaskInstance: The last task node in this section.
    """

    MACHINE_NAME = f"{dag.params['networks_vm']}-{{{{ ds.format('%Y%m01') }}}}"

    create_vm_neo4j = BashOperator(
        task_id="create_vm_neo4j",
        bash_command=f"gcloud beta compute instances create {MACHINE_NAME} "
        "--project {{ params['project'] }} "
        "--zone {{ params['instance_zone'] }} "
        "--machine-type {{ params['networks_machine_type'] }} "
        "--source-machine-image {{ params['networks_vm'] }} "
        "--labels pipeline={{ params['pipeline_label_value'] }} ",
        dag=dag,
    )

    def templated_operator_run(**context):
        from airflow.providers.google.cloud.hooks.compute_ssh import (
            ComputeEngineSSHHook,
        )
        from airflow.providers.ssh.operators.ssh import SSHOperator

        ssh_operator = SSHOperator(
            task_id="ssh_operator_python",
            ssh_hook=ComputeEngineSSHHook(
                instance_name=context["templates_dict"]["machine_name"],
                zone=dag.params["instance_zone"],
                use_oslogin=False,
                use_iap_tunnel=False,
                # use_internal_ip=True,
                cmd_timeout=None
            ),
            # command="cat /usr/bin/load_data.sh | sudo bash",
            command=context["templates_dict"]["command"],
        )
        return ssh_operator.execute(context=context)

    import_shell_scripts = PythonOperator(
        task_id="import_shell_scripts",
        python_callable=templated_operator_run,
        templates_dict={
            "machine_name": MACHINE_NAME,
            "command": "sudo bash /usr/bin/initialize.sh",
        },
        provide_context=True,
        dag=dag,
        retries=2,
        on_retry_callback=None,
        retry_delay=timedelta(minutes=1),
    )

    import_to_neo4j = PythonOperator(
        task_id="import_to_neo4j",
        python_callable=templated_operator_run,
        templates_dict={
            "machine_name": MACHINE_NAME,
            "command": "sudo bash '/usr/bin/load_data.sh' '{{ ds_nodash.format('%Y%m01') }}' '' 'dbprod'",
        },
        provide_context=True,
        dag=dag,
    )

    run_neo4j_admin = PythonOperator(
        task_id="run_neo4j_admin",
        python_callable=templated_operator_run,
        templates_dict={
            "machine_name": MACHINE_NAME,
            "command": "cat /usr/bin/run1.sh | sudo bash",
        },
        provide_context=True,
        dag=dag,
    )

    create_neo4j_database = PythonOperator(
        task_id="create_neo4j_database",
        python_callable=templated_operator_run,
        templates_dict={
            "machine_name": MACHINE_NAME,
            "command": "cat /usr/bin/run2.sh | sudo bash",
        },
        provide_context=True,
        dag=dag,
    )

    start_neo4j_database = PythonOperator(
        task_id="start_neo4j_database",
        python_callable=templated_operator_run,
        templates_dict={
            "machine_name": MACHINE_NAME,
            "command": "cat /usr/bin/run3.sh | sudo bash",
        },
        provide_context=True,
        dag=dag,
    )

    setlabel = PythonOperator(
        task_id="setlabel",
        python_callable=templated_operator_run,
        templates_dict={
            "machine_name": MACHINE_NAME,
            "command": "cat /usr/bin/run4.sh | sudo bash",
        },
        provide_context=True,
        dag=dag,
    )

    create_graph = PythonOperator(
        task_id="create_graph",
        python_callable=templated_operator_run,
        templates_dict={
            "machine_name": MACHINE_NAME,
            "command": "cat /usr/bin/run5.sh | sudo bash",
        },
        provide_context=True,
        dag=dag,
    )

    import_shell_scripts_train = PythonOperator(
        task_id="import_shell_scripts_train",
        python_callable=templated_operator_run,
        templates_dict={
            "machine_name": MACHINE_NAME,
            "command": "sudo bash /usr/bin/initialize.sh",
        },
        provide_context=True,
        dag=dag,
    )

    create_ml_pipeline = PythonOperator(
        task_id="create_ml_pipeline",
        python_callable=templated_operator_run,
        templates_dict={
            "machine_name": MACHINE_NAME,
            "command": "cat /usr/bin/run6.sh | sudo bash",
        },
        provide_context=True,
        dag=dag,
    )

    update_ml_pipeline = PythonOperator(
        task_id="update_ml_pipeline",
        python_callable=templated_operator_run,
        templates_dict={
            "machine_name": MACHINE_NAME,
            "command": "cat /usr/bin/run7.sh | sudo bash",
        },
        provide_context=True,
        dag=dag,
    )

    train = PythonOperator(
        task_id="train",
        trigger_rule=TriggerRule.NONE_SKIPPED,
        python_callable=templated_operator_run,
        templates_dict={
            "machine_name": MACHINE_NAME,
            "command": "cat /usr/bin/run8.sh | sudo bash",
        },
        provide_context=True,
        dag=dag,
    )

    predict = PythonOperator(
        task_id="predict",
        trigger_rule=TriggerRule.NONE_SKIPPED,
        python_callable=templated_operator_run,
        templates_dict={
            "machine_name": MACHINE_NAME,
            "command": "cat /usr/bin/run9.sh | sudo bash",
        },
        provide_context=True,
        dag=dag,
    )

    create_id = PythonOperator(
        task_id="create_id",
        trigger_rule=TriggerRule.NONE_SKIPPED,
        python_callable=templated_operator_run,
        templates_dict={
            "machine_name": MACHINE_NAME,
            "command": "cat /usr/bin/run10.sh | sudo bash",
        },
        provide_context=True,
        dag=dag,
    )

    export_edges_id = PythonOperator(
        task_id="export_edges_id",
        python_callable=templated_operator_run,
        templates_dict={
            "machine_name": MACHINE_NAME,
            "command": "sudo bash /usr/bin/run11.sh '{{ ds_nodash.format('%Y%m01') }}'",
        },
        provide_context=True,
        dag=dag,
    )

    delete_db = PythonOperator(
        task_id="delete_db",
        python_callable=templated_operator_run,
        templates_dict={
            "machine_name": MACHINE_NAME,
            "command": "sudo rm -rf /var/lib/neo4j/data/databases/db*",
        },
        provide_context=True,
        dag=dag,
    )

    delete_vm_neo4j = BashOperator(
        task_id="delete_vm_neo4j",
        bash_command=f"gcloud beta compute instances delete {MACHINE_NAME} "
        "--project {{ params['project'] }} "
        "--zone {{ params['instance_zone'] }}",
        dag=dag,
    )

    neo4j_end = DummyOperator(task_id="neo4j_end", dag=dag)
    graph_end = DummyOperator(task_id="graph_end", dag=dag)
    predict_end = DummyOperator(task_id="predict_end", dag=dag)

    (
        start
        >> create_vm_neo4j
        >> import_shell_scripts
        >> import_to_neo4j
        >> run_neo4j_admin
        >> create_neo4j_database
        >> start_neo4j_database
        >> neo4j_end
    )
    (
        neo4j_end
        >> setlabel
        >> create_graph
        >> graph_end
        >> [import_shell_scripts_train, create_id]
    )
    (
        import_shell_scripts_train
        >> create_ml_pipeline
        >> update_ml_pipeline
        >> train
        >> predict
        >> create_id
        >> export_edges_id
        >> delete_db
        >> delete_vm_neo4j
        >> predict_end
    )
    return predict_end
