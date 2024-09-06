from __future__ import annotations

import numpy as np
from dateutil.relativedelta import relativedelta

from airflow.models import TaskInstance, Variable
from airflow.utils.task_group import TaskGroup
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

from common.operators.bigquery import OlvinBigQueryOperator
from common.operators.exceptions import MarkSuccessOperator
from common.utils import callbacks

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
        depends_on_past=False,
        python_callable=set_first_monday,
        )
        start >> sensor >> push_first_monday

    return group
