from __future__ import annotations

from typing import List

from airflow.models import DAG, TaskInstance
from common.operators.bigquery import OlvinBigQueryOperator
from common.utils import queries


def register(start: TaskInstance, dag: DAG, event_tables: List[str]) -> TaskInstance:
    """
    Register tasks on the dag

    Args:
        dag (airflow.models.DAG): DAG instance to register tasks on.
        start (airflow.models.TaskInstance): Task instance all tasks will
        be registered downstream from.

    Returns:
        airflow.models.TaskInstance: The last task node in this section.
    """
    _queries = list()

    for table in event_tables:
        source = f"{{{{ var.value.env_project }}}}.{{{{ params['smc_events_dataset'] }}}}.{{{{ params['{table}'] }}}}"
        destination = f"{{{{ var.value.env_project }}}}.{{{{ params['events_dataset'] }}}}.{{{{ params['{table}'] }}}}"

        _queries.append(queries.copy_table(source, destination))

    dynamic_tasks = OlvinBigQueryOperator.partial(
            project_id="{{ var.value.dev_project_id }}",
        task_id="migrate_event_tables"
    ).expand(query=_queries)

    start >> dynamic_tasks

    return dynamic_tasks
