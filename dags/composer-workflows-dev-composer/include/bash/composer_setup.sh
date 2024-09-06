#!/usr/bin/env bash
# Https://stackoverflow.com/questions/430078/shell-script-templates
set -exo pipefail

# Sourcing the composer_settings.sh file and passing in the first argument.
source composer_settings.sh ${1}

# Setting the shell option to treat unset variables as an error and exit immediately.
set -u

# Setting the project to the dev project.
echo "Setting ${DEV_PROJECT_ID} as project"
gcloud config set project ${DEV_PROJECT_ID}

# Creating a new Composer environment.
echo "Creating ${COMPOSER_NAME} environment..."
gcloud composer environments create ${COMPOSER_NAME} \
    --impersonate-service-account=${DEV_SERVICE_ACCOUNT} \
    --project=${DEV_PROJECT_ID} \
    --location=${COMPOSER_LOCATION} \
    --airflow-configs=core-dags_are_paused_at_creation=True,core-dagbag_import_timeout=540,core-dag_file_processor_timeout=480,scheduler-catchup_by_default=False,core-max_active_runs_per_dag=1,core-test_connection=Enabled,webserver-show_trigger_form_if_no_params=True,api-enable_xcom_deserialize_support=True \
    --image-version=${IMAGE_VERSION} \
    --labels env=${ENVIRONMENT} \
    --environment-size=${ENVIRONMENT_SIZE} \
    --max-workers=${MAX_WORKERS} \
    --min-workers=${MIN_WORKERS} \
    --scheduler-count=${SCHEDULER_COUNT} \
    --scheduler-cpu=${SCHEDULER_CPU} \
    --scheduler-memory=${SCHEDULER_MEMORY} \
    --scheduler-storage=${SCHEDULER_STORAGE} \
    --web-server-cpu=${WEB_SERVER_CPU} \
    --web-server-memory=${WEB_SERVER_MEMORY} \
    --web-server-storage=${WEB_SERVER_STORAGE} \
    --worker-cpu=${WORKER_CPU} \
    --worker-memory=${WORKER_MEMORY} \
    --worker-storage=${WORKER_STORAGE}

COMPOSER_GCS_BUCKET=$(gcloud composer environments describe ${COMPOSER_NAME} --location ${COMPOSER_LOCATION} --impersonate-service-account=${DEV_SERVICE_ACCOUNT} | grep 'dagGcsPrefix' | grep -Eo "\S+/")
COMPOSER_GCS_BUCKET_DATA_FOLDER=${COMPOSER_GCS_BUCKET}data
COMPOSER_GCS_BUCKET_DAGS_FOLDER=${COMPOSER_GCS_BUCKET}dags
echo "Data folder is ${COMPOSER_GCS_BUCKET_DATA_FOLDER}"

# Copying the env_vars.json file from the prod GCS bucket to the composer environment's data folder.
gsutil rsync -r -d ${COMPOSER_BACKUP_BUCKET}/data ${COMPOSER_GCS_BUCKET_DATA_FOLDER}

# Importing the environment variables from JSON file.
echo "Importing environment variables..."
gcloud composer environments run ${COMPOSER_NAME} \
    --impersonate-service-account=${DEV_SERVICE_ACCOUNT} \
    --project=${DEV_PROJECT_ID} \
    --location ${COMPOSER_LOCATION} \
    variables -- import ${COMPOSER_INSTANCE_DATA_FOLDER}/${ENV_VARIABLES_JSON_NAME} 

gcloud composer environments run ${COMPOSER_NAME} \
    --impersonate-service-account=${DEV_SERVICE_ACCOUNT} \
    --project=${DEV_PROJECT_ID} \
    --location ${COMPOSER_LOCATION} \
    variables -- set "gcs_dags_folder" ${COMPOSER_GCS_BUCKET_DAGS_FOLDER}

# Importing the connections from the JSON file.
echo "Importing connections..."
gcloud composer environments run ${COMPOSER_NAME} \
    --impersonate-service-account=${DEV_SERVICE_ACCOUNT} \
    --project=${DEV_PROJECT_ID} \
    --location ${COMPOSER_LOCATION} \
    connections -- import ${COMPOSER_INSTANCE_DATA_FOLDER}/${CONNECTIONS_JSON_NAME}

# Uncomment if pypi dependencies needed in environment.
gsutil cp ${COMPOSER_BACKUP_BUCKET}/requirements.txt requirements.txt
# Installing the PyPi packages from the requirements.txt file.
echo "Importing pypi packages..."
gcloud composer environments update ${COMPOSER_NAME} \
    --impersonate-service-account=${DEV_SERVICE_ACCOUNT} \
    --project=${DEV_PROJECT_ID} \
    --location ${COMPOSER_LOCATION} \
    --update-pypi-packages-from-file requirements.txt

# Copying the DAGs from the backup bucket to the DAG folder.
echo "Importing DAGs..."
gsutil rsync -r -d ${COMPOSER_BACKUP_BUCKET}/dags ${COMPOSER_GCS_BUCKET_DAGS_FOLDER}

# Setting the project to the prod project.
echo "Setting ${PROD_PROJECT_ID} as project"
gcloud config set project ${PROD_PROJECT_ID}

echo "Done"