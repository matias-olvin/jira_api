from __future__ import annotations

from typing import List

from common import vars
# from common.operators.kubernetes_engine import GKEStartPodOperatorV2
from airflow.providers.google.cloud.operators.kubernetes_engine import GKEStartPodOperator

class BaseValidationOperator(GKEStartPodOperator):
    """
    A base class for operators that run validation jobs on a GKE cluster.
    """

    def __init__(
        self,
        task_id: str,
        validation: str,
        env: str,
        pipeline: str,
        step: str,
        run_date: str,
        source: str,
        destination: str,
        granularity: str = None,
        date_ranges: List[List[str]] = None,
        classes: List[str] = None,
        sub_validation: str = None,
        **kwargs,
    ) -> None:
        for arg in [validation, env, pipeline, step, run_date, source, destination]:
            if not isinstance(arg, str):
                raise TypeError(f"{arg} should be a string")

        if granularity is not None and not isinstance(granularity, str):
            raise TypeError("granularity should be a string")

        if sub_validation is not None and not isinstance(sub_validation, str):
            raise TypeError("sub_validation should be a string")

        if classes is not None:
            for arg in classes:
                if not isinstance(arg, str):
                    raise TypeError(f"{arg} in classes should be a string")

        if validation not in {"trend", "volume"}:
            raise ValueError(
                f"validation must be 'trend' or 'volume', not '{validation}'."
            )

        if sub_validation is not None and sub_validation not in {"poi", "group"}:
            raise ValueError(
                f"sub_validation must be 'poi' or 'group', not '{sub_validation}'."
            )

        self.validation = validation
        self.sub_validation = sub_validation
        self.env = env
        self.pipeline = pipeline
        self.run_date = run_date
        self.step = step
        self.source = source
        self.destination = destination
        self.granularity = granularity
        self.date_ranges = date_ranges
        self.classes = classes

        super().__init__(
            task_id=task_id,
            project_id=vars.SNS_PROJECT,
            location=vars.LOCATION_REGION,
            gcp_conn_id=vars.GCP_CONN_ID,
            cluster_name=vars.SNS_GKE_CLUSTER_NAME,
            namespace=vars.GCP_ACCESS_NAMESPACE,
            name=task_id,
            service_account_name=vars.GCP_ACCESS_SERVICE_ACCOUNT,
            image=vars.VALIDATIONS_IMAGE,
            image_pull_policy="Always",
            cmds=self._input_cmds(),
            startup_timeout_seconds=720,
            is_delete_operator_pod=True,
            get_logs=True,
            **kwargs,
        )

    def _input_cmds(self):
        cmds = [
            "python",
            "app.py",
            "--validation",
            self.validation,
            "--env",
            self.env,
            "--pipeline",
            self.pipeline,
            "--step",
            self.step,
            "--run_date",
            self.run_date,
            "--destination",
            self.destination,
            "--predicted_visits",
            self.source,
        ]

        if self.sub_validation is not None:
            cmds.extend(("--sub_validation", self.sub_validation))

        if self.granularity is not None:
            cmds.extend(("--granularity", self.granularity))

        if self.date_ranges is not None:
            for date_range in self.date_ranges:
                cmds.extend(("--date_ranges", date_range[0], date_range[1]))

        if self.classes is not None:
            for class_ in self.classes:
                cmds.extend(("--classes", class_))

        return cmds


class TrendValidationOperator(BaseValidationOperator):
    """
    An operators that runs trend validation jobs on a GKE cluster.
    """

    def __init__(
        self,
        task_id: str,
        sub_validation: str,
        env: str,
        pipeline: str,
        step: str,
        run_date: str,
        source: str,
        destination: str,
        granularity: str,
        date_ranges: List[List[str]],
        classes: List[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(
            task_id=task_id,
            validation="trend",
            sub_validation=sub_validation,
            env=env,
            pipeline=pipeline,
            step=step,
            run_date=run_date,
            source=source,
            destination=destination,
            granularity=granularity,
            date_ranges=date_ranges,
            classes=classes,
            **kwargs,
        )


class VolumeValidationOperator(BaseValidationOperator):
    def __init__(
        self,
        task_id: str,
        env: str,
        pipeline: str,
        step: str,
        run_date: str,
        source: str,
        destination: str,
        classes: List[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(
            task_id=task_id,
            validation="volume",
            env=env,
            pipeline=pipeline,
            step=step,
            run_date=run_date,
            source=source,
            destination=destination,
            classes=classes,
            **kwargs,
        )
