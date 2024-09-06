from __future__ import annotations

from airflow.models import DAG, TaskInstance
from airflow.operators.python import PythonOperator


def register(start: TaskInstance, dag: DAG, env: str) -> TaskInstance:
    def _push_value_to_xcom(env: str) -> str:
        """the function returns the correct dataset to use based on whether the env is prod or dev"""
        if env == "prod":
            return dag.params["smc_ground_truth_volume_dataset"]

        return dag.params["prior_brand_visits_dev_dataset"]

    determine_dataset_based_on_env = PythonOperator(
        task_id="push_dataset_name_to_xcom",
        python_callable=_push_value_to_xcom,
        op_kwargs={"env": env},
    )

    start >> determine_dataset_based_on_env

    return determine_dataset_based_on_env
