from __future__ import annotations

from datetime import datetime
from typing import Dict

from airflow.operators.bash import BashOperator

from common import vars


class DeleteSuccessFileOperator(BashOperator):
    """
    Creates a Task that deletes a _SUCCESS file from a given bucket and prefix.
    """

    def __init__(self, task_id: str, bucket: str, prefix: str, **kwargs) -> None:
        super().__init__(
            task_id=task_id,
            bash_command=self._bash_command(),
            env={"FILEPATH": f"gs://{bucket}/{prefix}/_SUCCESS"},
            **kwargs,
        )

    @staticmethod
    def _bash_command() -> str:
        """
        Checks the status of a file using `gsutil` and deletes the file if it exists.

        Returns:
          a bash command as a string.
        """
        file_status = "$(gsutil -q stat $FILEPATH ; echo $?)"
        status_0 = "gsutil -m rm -f $FILEPATH"
        status_1 = 'echo "file $FILEPATH not found"'

        return f'if [ "{file_status}" = 0 ]; then {status_0}; else {status_1}; fi'


class WriteSuccessFileOperator(BashOperator):
    """
    Creates a Task that writes a _SUCCESS file to a given bucket and prefix.
    """

    def __init__(self, task_id: str, bucket: str, prefix: str, **kwargs) -> None:
        self.bucket = bucket
        self.prefix = prefix
        super().__init__(
            task_id=task_id,
            bash_command=self._bash_command(),
            env={"FILEPATH": f"gs://{bucket}/{prefix}"},
            **kwargs,
        )

    def _bash_command(self) -> str:
        """
        Creates a file called in a specified file path using `gsutil`.

        Returns:
          a bash command as a string.
        """
        return "touch _SUCCESS; gsutil -m mv _SUCCESS $FILEPATH"


class SNSTriggerDAGRunOperator(BashOperator):
    """
    Creates a Task to trigger DAGs from the SNS Composer Instance.
    """

    def __init__(
        self,
        task_id: str,
        trigger_dag_id: str,
        conf: Dict = None,
        execution_date: str = None,
        reset_dag_run: bool = False,
        **kwargs,
    ) -> None:
        self.trigger_dag_id = trigger_dag_id
        self.conf = str(conf) if conf is not None else "{}"
        self.execution_date = (
            execution_date
            if execution_date is not None
            else datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        )
        self.reset_dag_run = reset_dag_run
        super().__init__(
            task_id=task_id,
            bash_command=self._bash_command(),
            env={
                "COMPOSER": vars.SNS_COMPOSER_ENV,
                "PROJECT": vars.SNS_PROJECT,
                "LOCATION": vars.SNS_COMPOSER_ZONE,
                "SERVICE_ACCOUNT": vars.CROSS_PROJECT_SA,
                "DAG_ID": self.trigger_dag_id,
                "EXEC_DATE": self.execution_date,
                "CONF": self.conf,
            },
            append_env=True,
            **kwargs,
        )

    @staticmethod
    def _dag_state() -> str:
        """
        Returns the state of a DAG.

        Returns:
          a string that contains a command to be executed in the shell.
        """
        return (
            "echo $(gcloud composer environments run $COMPOSER "
            "--project $PROJECT --location $LOCATION "
            "--impersonate-service-account $SERVICE_ACCOUNT "
            "dags state -- $DAG_ID $EXEC_DATE) "
            "| awk '{print $NF}'"
        )

    @staticmethod
    def _dag_clear() -> str:
        """
        Clears an existing DAG Run.

        Returns:
          a string that contains a command to be executed in the shell.
        """
        return (
            "gcloud composer environments run $COMPOSER "
            "--project $PROJECT --location $LOCATION "
            "--impersonate-service-account $SERVICE_ACCOUNT "
            "tasks clear -- -d --yes -s $EXEC_DATE -e $EXEC_DATE $DAG_ID"
        )

    @staticmethod
    def _dag_trigger() -> str:
        """
        Triggers a DAG Run.

        Returns:
          a string that contains a command to be executed in the shell.
        """
        return (
            "gcloud composer environments run $COMPOSER "
            "--project $PROJECT --location $LOCATION "
            "--impersonate-service-account $SERVICE_ACCOUNT "
            "dags trigger -- -c $CONF -e $EXEC_DATE $DAG_ID"
        )

    def _bash_command(self) -> str:
        """
        Triggers a DAG Run or clears an exisitng DAG Run.

        Returns:
          a string that contains a command to be executed in the shell.
        """
        command = (
            f"{self._dag_clear()}"
            if self.reset_dag_run
            else 'echo "Error: Dag Run already exists." '
        )

        return (
            f"if [ $({self._dag_state()}) == None ]; then "
            f"{self._dag_trigger()}; "
            "else "
            f"{command}; "
            "fi"
        )
