import os

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from common.utils import dag_args

path = f"{os.path.dirname(os.path.realpath(__file__))}"
DAG_ID = path.split("/")[-1]

env_args = dag_args.make_env_args(
    schedule_interval="@weekly",
)
default_args=dag_args.make_default_args(
    start_date=pendulum.datetime(2022, 9, 20, tz="Europe/London"),
    retries=3,
)

with DAG(
    DAG_ID,
    default_args=default_args,
    description="Delete untagged images from artifact registry",
    schedule_interval=env_args["schedule_interval"],  # @weekly
    tags=[env_args["env"], "weekly"],
    params=dag_args.load_config(scope="global"),
    doc_md=dag_args.load_docs(__file__),
) as dag:
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    def bash_command(
        project: str, repository: str, location: str = "europe-west1"
    ) -> str:
        """
        It returns a string that is a bash command that lists all the images in a repository, filters
        out the ones that have tags, and then deletes them

        Args:
          project: The project ID of the project that contains the repository.
          repository: The name of the repository you want to delete.
          location: The location of the repository. Defaults to europe-west1

        Returns:
          A string
        """
        return (
            "gcloud artifacts docker images list "
            f"{location}-docker.pkg.dev/{project}/{repository} "
            "--filter='-tags:*' "
            '--format="value(format("{0}@{1}",package, version))" '
            "--include-tags | "
            "xargs -I {image_path} "
            "gcloud artifacts docker images delete {image_path}"
        )

    for REPOSITORY in dag.params["artifact-registry-prod-repos"]:
        PROJECT = Variable.get("prod_project")
        bash_task = BashOperator(
            task_id=f"pruning-prod-{REPOSITORY}",
            bash_command=bash_command(PROJECT, REPOSITORY),
        )
        start >> bash_task >> end

    for REPOSITORY in dag.params["artifact-registry-dev-repos"]:
        PROJECT = Variable.get("dev_project")
        bash_task = BashOperator(
            task_id=f"pruning-dev-{REPOSITORY}",
            bash_command=bash_command(PROJECT, REPOSITORY),
        )
        start >> bash_task >> end
