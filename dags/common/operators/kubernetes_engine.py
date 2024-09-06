from __future__ import annotations
from typing import Sequence
from functools import cached_property

from airflow.providers.google.cloud.operators.kubernetes_engine import GKEStartPodOperator

from common.hooks.kubernetes_engine import GKEPodHookV2

class GKEStartPodOperatorV2(GKEStartPodOperator):
    def __init__(
        self,
        *,
        location: str,
        cluster_name: str,
        use_internal_ip: bool = False,
        project_id: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        regional: bool | None = None,
        on_finish_action: str | None = None,
        is_delete_operator_pod: bool | None = None,
        **kwargs,
    ) -> None:
        super().__init__(
            location=location,
            cluster_name=cluster_name,
            use_internal_ip=use_internal_ip,
            project_id=project_id,
            gcp_conn_id=gcp_conn_id,
            impersonation_chain=impersonation_chain,
            regional=regional,
            on_finish_action=on_finish_action,
            is_delete_operator_pod=is_delete_operator_pod,
            **kwargs,
        )

    @cached_property
    def hook(self) -> GKEPodHookV2:
        if self._cluster_url is None or self._ssl_ca_cert is None:
            raise AttributeError(
                "Cluster url and ssl_ca_cert should be defined before using self.hook method. "
                "Try to use self.get_kube_creds method",
            )

        hook = GKEPodHookV2(
            cluster_url=self._cluster_url,
            ssl_ca_cert=self._ssl_ca_cert,
            gcp_conn_id=self.gcp_conn_id,
        )
        return hook