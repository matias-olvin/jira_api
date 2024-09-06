#!/usr/bin/env bash

ENVIRONMENT=$1
# Checking if the first argument is dev. If it is not, it will exit with error code 99.
case ${ENVIRONMENT} in
    dev)
        COMPOSER_NAME=${ENVIRONMENT}
        ;;
    *)
        echo "usage: ./composer_setup.sh {dev}" 1>&2
        exit 99
        ;;
esac

# Setting up the environment variables for the script.
DEV_PROJECT_ID=storage-dev-olvin-com
PROD_PROJECT_ID=storage-prod-olvin-com

COMPOSER_INSTANCE_DATA_FOLDER=/home/airflow/gcs/data
COMPOSER_LOCATION=europe-west1
IMAGE_VERSION=composer-2.8.4-airflow-2.7.3
DEV_SERVICE_ACCOUNT=414729340654-compute@developer.gserviceaccount.com
ENVIRONMENT_SIZE=medium
MAX_WORKERS=6
MIN_WORKERS=2
SCHEDULER_COUNT=2
SCHEDULER_CPU=2
SCHEDULER_MEMORY=7.5GB
SCHEDULER_STORAGE=5GB
WEB_SERVER_CPU=2
WEB_SERVER_MEMORY=7.5GB
WEB_SERVER_STORAGE=5GB
WORKER_CPU=2
WORKER_MEMORY=7.5GB
WORKER_STORAGE=5GB
echo "Operating on environment ${COMPOSER_NAME}" 1>&2

COMPOSER_BACKUP_BUCKET=gs://dev-composer-backup-olvin-com
# CREDENTIALS_FOLDER=${COMPOSER_BACKUP_BUCKET}/credentials

ENV_VARIABLES_JSON_NAME=env_var.json
# ENV_VARIABLES_JSON_GCS_LOCATION=${CREDENTIALS_FOLDER}/${ENV_VARIABLES_JSON_NAME}

CONNECTIONS_JSON_NAME=connections.json
# CONNECTIONS_JSON_GCS_LOCATION=${CREDENTIALS_FOLDER}/${CONNECTIONS_JSON_NAME}
