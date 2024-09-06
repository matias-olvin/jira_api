from __future__ import annotations

from functools import cached_property
from typing import Any, Dict

from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.kubernetes_engine import (
    GKEStartPodOperator,
)
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from common import vars
from common.hooks.kubernetes_engine import GKEPodHook


class OlvinBigQueryOperator(BigQueryInsertJobOperator):
    def __init__(
        self,
        task_id: str,
        query: str,
        billing_tier: str = "low",
        **kwargs,
    ) -> None:
        self.configuration = {
            "query": {
                "query": query,
                "useLegacySql": "false",
                "maximumBytesBilled": self.set_max_bytes_billed(billing_tier),
            },
            "labels": {
                "pipeline": "{{ dag.dag_id.lower()[:63] |  replace('.','-') }}",
                "task_id": "{{ task.task_id.lower()[:63] |  replace('.','-') }}",
            },
        }
        self.deferrable=True
        self.poll_interval=10.0
        self.location = vars.LOCATION_MULTI_REGION
        super().__init__(
            task_id=task_id,
            configuration=self.configuration,
            location=self.location,
            **kwargs,
        )

    @staticmethod
    def set_max_bytes_billed(billing_tier: str) -> str:
        if billing_tier == "low":
            return "100000000000"  # 100 GB
        elif billing_tier == "med":
            return "500000000000"  # 500 GB
        elif billing_tier == "high":
            return "5000000000000"  # 5 TB
        elif billing_tier == "higher":
            return "15000000000000"  # 15 TB
        elif billing_tier == "highest":
            return "25000000000000"  # 25 TB
        elif billing_tier == "peak":
            return "50000000000000"  # 25 TB

    def execute(self, context: Dict[str, Any]) -> None:
        super().execute(context)
        if context["dag_run"] and context["task_instance"]:
            if context["task_instance"].state == "failed":
                self.log.info("Task failed, cancelling BigQuery job.")
                self.cancel(context)

    def cancel(self, context: Dict[str, Any]) -> None:
        # Cancel the BigQuery job
        bq_client = bigquery.Client(project=self.project_id)
        try:
            bq_client.cancel_job(context["task_instance"].job_id)
        except NotFound:
            # Handle the case where the job has already completed or does not exist
            pass


class SNSBigQueryOperator(GKEStartPodOperator):
    def __init__(
        self,
        task_id: str,
        query: str,
        **kwargs,
    ) -> None:
        super().__init__(
            task_id=task_id,
            project_id=vars.SNS_PROJECT,
            location=vars.LOCATION_REGION,
            gcp_conn_id=vars.GCP_CONN_ID,
            cluster_name=vars.SNS_GKE_CLUSTER_NAME,
            namespace=vars.GCP_ACCESS_NAMESPACE,
            service_account_name=vars.GCP_ACCESS_SERVICE_ACCOUNT,
            image=vars.SNS_BIGQUERY_IMAGE,
            image_pull_policy="Always",
            cmds=["python", "sns_bigquery/main.py", "--query", query],
            name=task_id,
            startup_timeout_seconds=720,
            is_delete_operator_pod=True,
            get_logs=True,
            **kwargs,
        )

    @cached_property
    def hook(self) -> GKEPodHook:
        if self._cluster_url is None or self._ssl_ca_cert is None:
            raise AttributeError(
                "Cluster url and ssl_ca_cert should be defined before using self.hook"
                " method. Try to use self.get_kube_creds method" 
            )
        
        hook = GKEPodHook(
            gcp_conn_id=self.gcp_conn_id,
            cluster_url=self._cluster_url,
            ssl_ca_cert=self._ssl_ca_cert,
        )
        return hook
