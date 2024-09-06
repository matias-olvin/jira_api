from __future__ import annotations

from kubernetes import client
from airflow.providers.google.cloud.hooks.kubernetes_engine import GKEPodHook
from kubernetes_asyncio.config.kube_config import FileOrData

class GKEPodHookV2(GKEPodHook):
    """Google Kubernetes Engine pod APIs."""
    def __init__(
        self,
        cluster_url: str,
        ssl_ca_cert: str,
        *args,
        **kwargs,
    ):
        super().__init__(
            cluster_url=cluster_url,
            ssl_ca_cert=ssl_ca_cert,
            *args,
            **kwargs
        )
        self._cluster_url = cluster_url
        self._ssl_ca_cert = ssl_ca_cert
        
    def get_conn(self) -> client.ApiClient:
        configuration = self._get_config()
        configuration.refresh_api_key_hook = self._refresh_api_key_hook
        return client.ApiClient(configuration)

    def _refresh_api_key_hook(self, configuration: client.configuration.Configuration):
        configuration.api_key = {"authorization": self._get_token(self.get_credentials())}

    def _get_config(self) -> client.configuration.Configuration:
        configuration = client.Configuration(
            host=self._cluster_url,
            api_key_prefix={"authorization": "Bearer"},
            api_key={"authorization": self._get_token(self.get_credentials())},
        )
        configuration.ssl_ca_cert = FileOrData(
            {
                "certificate-authority-data": self._ssl_ca_cert,
            },
            file_key_name="certificate-authority",
        ).as_file()
        return configuration