from __future__ import annotations

import numpy as np
from airflow.models import TaskInstance, Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
from dateutil.relativedelta import relativedelta


def register(dag, start: TaskInstance) -> TaskGroup:
    """
    Register tasks on the dag

    Args:
        start (airflow.models.TaskInstance): Task instance all tasks will
        be registered downstream from.

    Returns:
        airflow.utils.task_group.TaskGroup: The task group in this section.
    """

    def set_first_monday(**context) -> None:
        """
        The function takes the logical date of the dag run and uses the numpy busday_offset function
        to find the first Monday of the month
        """
        run_date = context["data_interval_end"]
        first_monday = f"{np.busday_offset(run_date.strftime('%Y-%m'), 0, roll='forward', weekmask='Mon')}"
        Variable.set("first_monday", first_monday)

    with TaskGroup(group_id="common") as group:

        sensor = ExternalTaskSensor(
            task_id="placekey_sensor",
            external_dag_id="{{ params['placekeys_feed'] }}",
            external_task_id="end",
            execution_date_fn=lambda dt: (dt + relativedelta(weeks=1)).replace(day=1, hour=6) - relativedelta(months=1),
            mode="reschedule",
            poke_interval=60 * 5,
            timeout=60 * 60 * 24 * 3, # 3 days timeout
        )

        push_first_monday = PythonOperator(
            task_id="push_first_monday",
            python_callable=set_first_monday,
        )

        turn_off_requester_pays_gcs = BashOperator(
            task_id="turn_off_requester_pays_gcs",
            bash_command=f"gcloud storage buckets update "
        f"gs://{{{{ params['feeds_staging_gcs_bucket'] }}}} "
        f"--no-requester-pays --billing-project={{{{ params['data_feeds_project'] }}}}",
        )

        start >> sensor >> push_first_monday >> turn_off_requester_pays_gcs

    return group
