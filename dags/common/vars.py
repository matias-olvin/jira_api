from airflow.models import Variable

LOCATION_MULTI_REGION = "EU"
LOCATION_REGION = "europe-west1"
LOCATION_ZONE = f"{LOCATION_REGION}-b"

OLVIN_PROJECT = Variable.get("env_project")
ACCESSIBLE_BY_SNS = "accessible_by_sns"

SNS_PROJECT = Variable.get("sns_project")
SNS_RAW_DATASET = "sns_raw"
ACCESSIBLE_BY_OLVIN = Variable.get("accessible_by_olvin")

SNS_COMPOSER_ENV = "prod-sensormatic"
SNS_COMPOSER_ZONE = LOCATION_REGION
SNS_GKE_CLUSTER_NAME = "europe-west1-prod-sensormat-9506e385-gke"
GCP_ACCESS_NAMESPACE = "gcp-access-sa"
GCP_ACCESS_SERVICE_ACCOUNT = "gcp-access-sa"
GCP_CONN_ID = "cross_project_worker_conn"

SNS_SA_SUFFIX = "sns-vendor-olvin-poc.iam.gserviceaccount.com"
CROSS_PROJECT_SA = f"cross-project-composer-worker@{SNS_SA_SUFFIX}"


SNS_REGISTRY = "europe-west1-docker.pkg.dev/sns-vendor-olvin-poc"
SNS_BIGQUERY_IMAGE = f"{SNS_REGISTRY}/connections/sns_bigquery:latest"
VALIDATIONS_IMAGE = f"{SNS_REGISTRY}/validations/validation_modules:latest"

GOOGLE_SHEETS_IMAGE = "europe-west1-docker.pkg.dev/storage-prod-olvin-com/connections/google_sheets:latest"
OLVIN_GKE_CLUSTER_NAME = Variable.get("gke_cluster_name")
COMPOSER_NAME = Variable.get("composer_name")