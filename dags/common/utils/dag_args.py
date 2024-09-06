from __future__ import annotations

import os
from datetime import timedelta
from pathlib import Path
from typing import Dict

import yaml
from airflow.models import Variable


def make_default_args(**kwargs):
    """
    Make a new default argument dictionary from base arguments and kwargs.

    Args:
        kwargs (optional): Additional default arguments to use.
    """
    return {
        "owner": "airflow",
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "wait_for_downstream": False,
        "retry_delay": timedelta(minutes=5),
        **kwargs,
    }


def make_env_args(schedule_interval: str = None, dag_id: str = None, folder: str = None) -> Dict:
    """
    It takes a folder name, a DAG ID, and a schedule interval, and returns a
    dictionary of DAG arguments

    Args:
      folder (str): The folder where the DAG is located.
      dag_id (str): The name of the DAG.
      schedule_interval (str): The schedule interval for the DAG.

    Returns:
      A dictionary with the dag_id, schedule_interval, and env.
    """
    dag_args = {
        "dag_id": dag_id,
        "schedule_interval": schedule_interval,
        "env": "prod",
    }

    if 'AIRFLOW_HOME' in os.environ:
        prod = "prod" in Variable.get("env_project")
    else:
        prod = False

    if not prod:
        dag_args["schedule_interval"] = None
        dag_args["env"] = "dev"

    return dag_args


def load_config(
    caller_path: str = None, path: str = "config.yaml", scope: str = "local"
) -> Dict[str, str]:
    """
    It loads a yaml file relative to the caller file

    Args:
      caller_path: Absolute path of the caller file. Typically
        passed with `__file__`.
      path: Relative path to the configuration file from
        `caller_path`. Defaults to `config.yaml`.
      scope: local or global. Defaults to local

    Returns:
      The parsed configuration dictionary.
    """
    if scope == "global":
        if 'AIRFLOW_HOME' in os.environ:
            filename = "/home/airflow/gcs/dags/common/config.yaml"
        else:
            filename = "dags/common/config.yaml"
            
        with open(filename, "r") as config:
            return yaml.load(config, Loader=yaml.FullLoader)

    elif scope == "local":
        caller_dir = Path(caller_path).parent
        with Path(caller_dir, path).open() as config:
            return yaml.load(config, Loader=yaml.FullLoader)


def load_docs(caller_path: str, path: str = "README.md") -> str:
    """Load a markdown file as a string relative from the caller_path.

    Args:
        caller_path (string): Absolute path of the caller file. Typically
            passed with `__file__`.
        path (string, optional): Relative path to the docs file from
            `caller_path`. Defaults to `README.md`.

    Returns:
        Dict: The loaded markdown string.
    """
    caller_dir = Path(caller_path).parent

    with Path(caller_dir, path).open() as file:
        return file.read()
