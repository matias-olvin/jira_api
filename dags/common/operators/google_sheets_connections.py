from common import vars
# from common.operators.kubernetes_engine import GKEStartPodOperatorV2
from airflow.providers.google.cloud.operators.kubernetes_engine import GKEStartPodOperator

import subprocess
bash_command = f"gcloud composer environments describe {vars.COMPOSER_NAME} --location=europe-west1 --format='value(config.gkeCluster)' | awk -F'/' '{{print $NF}}'"
process = subprocess.Popen(bash_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
output, error = process.communicate()
cluster_name = output.decode().strip()

class GoogleSheetsToGCSOperator(GKEStartPodOperator):
    def __init__(
        self,
        task_id: str,
        uri: str,
        spreadsheet_id: str,
        worksheet_title: str = "Sheet1",
        **kwargs,
    ) -> None:

        if not uri.startswith("gs://"):
            uri = "gs://" + uri

        self.uri = uri
        self.worksheet_title = worksheet_title
        self.spreadsheet_id = spreadsheet_id

        super().__init__(
            task_id=task_id,
            project_id=vars.OLVIN_PROJECT,
            location=vars.LOCATION_REGION,
            cluster_name=cluster_name,
            name=task_id,
            image=vars.GOOGLE_SHEETS_IMAGE,
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
            "google_sheets/app.py",
            "--uri",
            self.uri,
            "--spreadsheet_id",
            self.spreadsheet_id,
            "--worksheet_title",
            self.worksheet_title,
            "--reverse",
        ]

        return cmds


class GCSToGoogleSheetsOperator(GKEStartPodOperator):
    def __init__(
        self,
        task_id: str,
        uri: str,
        spreadsheet_id: str,
        worksheet_title: str = "Sheet1",
        clear_worksheet: bool = True,
        **kwargs,
    ) -> None:

        if not uri.startswith("gs://"):
            uri = "gs://" + uri

        self.uri = uri
        self.worksheet_title = worksheet_title
        self.clear_worksheet = clear_worksheet
        self.spreadsheet_id = spreadsheet_id

        super().__init__(
            task_id=task_id,
            project_id=vars.OLVIN_PROJECT,
            location=vars.LOCATION_REGION,
            cluster_name=cluster_name,
            name=task_id,
            image=vars.GOOGLE_SHEETS_IMAGE,
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
            "google_sheets/app.py",
            "--uri",
            self.uri,
            "--spreadsheet_id",
            self.spreadsheet_id,
            "--worksheet_title",
            self.worksheet_title,
        ]

        if self.clear_worksheet:
            cmds.extend(["--clear_worksheet"])

        return cmds
