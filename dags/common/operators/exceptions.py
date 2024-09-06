from __future__ import annotations

from typing import Any

from airflow.exceptions import AirflowFailException
from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context


class MarkSuccessOperator(BaseOperator):
    """
    Creates a Task that fails and must manually be marked success.
    """

    def __init__(self, task_id: str, **kwargs) -> None:
        super().__init__(
            task_id=task_id,
            retries=0,
            **kwargs,
        )

    def execute(self, context: Context) -> Any:
        """
        Raises an exception that will cause the task to fail.
        """
        raise AirflowFailException(
            "This task must be manually set to success to continue."
        )
