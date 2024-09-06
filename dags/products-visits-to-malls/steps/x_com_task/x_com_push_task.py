from __future__ import annotations

from airflow.models import TaskInstance
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup


def register(start: TaskInstance, env: str) -> TaskGroup:
    with TaskGroup(group_id="set_xcom_values") as group:

        def _push_value_to_xcom(env: str, value: str):
            if env == "prod":
                return value

            return value.replace("prod", "dev") if "prod" in value else f"{value}_dev"

        def push_xcom_values(*args):
            return [
                PythonOperator(
                    task_id=f"get_{value}",
                    python_callable=_push_value_to_xcom,
                    op_kwargs={"env": env, "value": f"{{{{ params['{value}'] }}}}"},
                )
                for value in args
            ]

        start >> push_xcom_values("accessible_by_olvin_dataset", "poi_matching_dataset")

    return group
