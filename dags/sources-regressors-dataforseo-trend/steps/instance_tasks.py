from __future__ import annotations

from airflow.models import TaskInstance
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.hooks.compute_ssh import ComputeEngineSSHHook
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.utils.task_group import TaskGroup
from common import vars

ZONE = vars.LOCATION_ZONE
PROJECT = vars.OLVIN_PROJECT
INSTANCE = "pby-source-p-cin-euwe1b-dataforseo-trend"
REPOSITORY = "pby-source-p-ary-euwe1-apis"
IMAGE = (
    f"{vars.LOCATION_REGION}-docker.pkg.dev/{PROJECT}/{REPOSITORY}/dataforseo:latest"
)
ENDPOINT = "trend"


def register(start: TaskInstance) -> TaskInstance:
    create = BashOperator(
        task_id="instance-create",
        bash_command="{% include './include/bash/instance-create.sh' %}",
        env={
            "INSTANCE": INSTANCE,
            "PROJECT": PROJECT,
            "ZONE": ZONE,
            "MACHINE_TYPE": "n2-standard-2",
            "IMAGE_FAMILY": "centos-stream-9",
            "IMAGE_PROJECT": "centos-cloud",
            "SCOPES": "https://www.googleapis.com/auth/cloud-platform",
        },
    )
    start >> create

    with TaskGroup(group_id="docker") as group:
        docker_install = SSHOperator(
            task_id="install",
            ssh_hook=ComputeEngineSSHHook(
                instance_name=INSTANCE,
                zone=ZONE,
                project_id=PROJECT,
                use_oslogin=True,
                use_iap_tunnel=False,
                use_internal_ip=True,
            ),
            command="{% include './include/bash/docker-install.sh' %}",
            cmd_timeout=None,
        )
        docker_run_post = SSHOperator(
            task_id="run-post",
            ssh_hook=ComputeEngineSSHHook(
                instance_name=INSTANCE,
                zone=ZONE,
                project_id=PROJECT,
                use_oslogin=True,
                use_iap_tunnel=False,
                use_internal_ip=True,
            ),
            command="{% include './include/bash/docker-run-post.sh' %}",
            cmd_timeout=None,
            params={
                "IMAGE": IMAGE,
                "ENDPOINT": ENDPOINT,
            },
            retries=0,
        )
        docker_run_get = SSHOperator(
            task_id="run-get",
            ssh_hook=ComputeEngineSSHHook(
                instance_name=INSTANCE,
                zone=ZONE,
                project_id=PROJECT,
                use_oslogin=True,
                use_iap_tunnel=False,
                use_internal_ip=True,
            ),
            command="{% include './include/bash/docker-run-get.sh' %}",
            cmd_timeout=None,
            params={
                "IMAGE": IMAGE,
                "ENDPOINT": ENDPOINT,
            },
            retries=0,
        )
        docker_run_transform = SSHOperator(
            task_id="run-transform",
            ssh_hook=ComputeEngineSSHHook(
                instance_name=INSTANCE,
                zone=ZONE,
                project_id=PROJECT,
                use_oslogin=True,
                use_iap_tunnel=False,
                use_internal_ip=True,
            ),
            command="{% include './include/bash/docker-run-transform.sh' %}",
            cmd_timeout=None,
            params={
                "IMAGE": IMAGE,
                "ENDPOINT": ENDPOINT,
            },
            retries=0,
        )

        (docker_install >> docker_run_post >> docker_run_get >> docker_run_transform)

    delete = BashOperator(
        task_id="instance-delete",
        bash_command="{% include './include/bash/instance-delete.sh' %}",
        env={
            "INSTANCE": INSTANCE,
            "PROJECT": PROJECT,
            "ZONE": ZONE,
        },
    )
    create >> group >> delete

    return delete
