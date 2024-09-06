from __future__ import annotations

from airflow.models import TaskInstance, Variable
from airflow.utils.task_group import TaskGroup
from common.operators.bigquery import OlvinBigQueryOperator, SNSBigQueryOperator
from common.operators.exceptions import MarkSuccessOperator
from common.utils import queries
from common.utils.callbacks import task_end_custom_slack_alert
from common.utils.formatting import accessible_table_name_format


def register(start: TaskInstance, env: str) -> TaskInstance:

    mark_success_to_continue_with_model_training = MarkSuccessOperator(
        task_id="mark_success_to_continue_with_model_training",
        on_failure_callback=task_end_custom_slack_alert(
            "U05FN3F961X",  # IGNACIO
            "U05M60N8DMX",  # MATIAS
            channel=env,
            msg_title="Determine if the model needs to run again. Check DAG documentation for instructions.",
        ),
    )

    create_model_task = SNSBigQueryOperator(
        task_id="create_ml_model",
        query="{% include './include/bigquery/model_creation_migration/create_model.sql' %}",
    )

    evaluate_model = SNSBigQueryOperator(
        task_id="evaluate_ml_model",
        query="{% include './include/bigquery/model_creation_migration/evaluate_model_query.sql' %}",
    )

    run_model_task = SNSBigQueryOperator(
        task_id="run_ml_model_task",
        query="{% include './include/bigquery/model_creation_migration/run_model.sql' %}",
    )

    with TaskGroup("copy_model_evaluation_to_olvin") as group_model_eval:
        source = "{{ params['sns-project-id'] }}.{{ ti.xcom_pull(task_ids='set_xcom_values.get_accessible_by_olvin_dataset') }}.{{ params['model_evaluation_visits_to_malls_table'] }}"

        destination_table = "{{ params['model_evaluation_visits_to_malls_table'] }}_{{ dag_run.conf['dataset_postgres_template'] }}"
        destination_dataset = "{{ params['visits_to_malls_dataset'] }}"

        destination = (
            f"{Variable.get('env_project')}.{destination_dataset}.{destination_table}"
        )

        staging = accessible_table_name_format(
            "olvin", table=destination_table, dataset=destination_dataset
        )

        copy_to_accessible = SNSBigQueryOperator(
            task_id="copy_model_evaluation_to_accessible_by_olvin",
            query=queries.copy_table(source, staging),
        )

        copy_to_working = OlvinBigQueryOperator(
            task_id="copy_model_evaluation_to_working_by_olvin",
            query=queries.copy_table(staging, destination),
        )

        drop_staging = SNSBigQueryOperator(
            task_id="delete_model_evaluation_from_accessible_by_olvin",
            query=queries.drop_table(staging),
        )

        (run_model_task >> copy_to_accessible >> copy_to_working >> drop_staging)

    with TaskGroup(group_id="copy_model_output_to_olvin") as group_model_output:
        source = "{{ params['sns-project-id'] }}.{{ ti.xcom_pull(task_ids='set_xcom_values.get_accessible_by_olvin_dataset') }}.{{ params['visits_to_malls_model_output'] }}"

        destination_table = "{{ params['visits_to_malls_model_output'] }}_{{ dag_run.conf['dataset_postgres_template'] }}"
        destination_dataset = "{{ params['visits_to_malls_dataset'] }}"

        destination = (
            f"{Variable.get('env_project')}.{destination_dataset}.{destination_table}"
        )

        staging = accessible_table_name_format(
            "olvin", table=destination_table, dataset=destination_dataset
        )

        copy_to_accessible = SNSBigQueryOperator(
            task_id="copy_model_output_to_accessible_by_olvin",
            query=queries.copy_table(source, staging),
        )

        copy_to_working = OlvinBigQueryOperator(
            task_id="copy_model_output_to_working_by_olvin",
            query=queries.copy_table(staging, destination),
        )

        drop_staging = SNSBigQueryOperator(
            task_id="delete_model_output_from_accessible_by_olvin",
            query=queries.drop_table(staging),
        )

        (group_model_eval >> copy_to_accessible >> copy_to_working >> drop_staging)

    (
        start
        >> mark_success_to_continue_with_model_training
        >> create_model_task
        >> evaluate_model
        >> run_model_task
        >> group_model_eval
        >> group_model_output
    )

    return group_model_output
