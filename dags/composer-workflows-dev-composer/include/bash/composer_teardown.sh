#!/usr/bin/env bash
# - `set -e`: Exit immediately if a command exits with a non-zero status.
# - `set -x`: Print commands and their arguments as they are executed.
# - `set -o pipefail`: The return value of a pipeline is the status of the last command to exit with a
# non-zero status, or zero if no command exited with a non-zero status.
# - `set -u`: Treat unset variables as an error when substituting.
set -exo pipefail

# Sourcing the composer_settings.sh file and passing in the first argument.
source composer_settings.sh ${1}

# Treat unset variables as an error when substituting.
set -u
 
# Setting the project to the dev project.
gcloud config set project ${DEV_PROJECT_ID}
echo "Project set to ${DEV_PROJECT_ID}"

# Getting the GCS bucket name from the Composer environment.
COMPOSER_GCS_BUCKET=$(gcloud composer environments describe ${COMPOSER_NAME} --location europe-west1 --impersonate-service-account=${DEV_SERVICE_ACCOUNT} | grep 'dagGcsPrefix' | grep -Eo "\S+/")
COMPOSER_GCS_BUCKET_DATA_FOLDER=${COMPOSER_GCS_BUCKET}data
COMPOSER_GCS_BUCKET_DAGS_FOLDER=${COMPOSER_GCS_BUCKET}dags

# Exporting the variables.json file from the Composer instance to the GCS bucket.
echo "Exporting environment variables..."
gcloud composer environments run ${COMPOSER_NAME} \
    --impersonate-service-account=${DEV_SERVICE_ACCOUNT} \
    --location=${COMPOSER_LOCATION} \
    variables -- export ${COMPOSER_INSTANCE_DATA_FOLDER}/${ENV_VARIABLES_JSON_NAME} 

# Exporting the connections.json file from the Composer instance to the GCS bucket.
echo "Exporting connections..."
gcloud composer environments run ${COMPOSER_NAME} \
    --impersonate-service-account=${DEV_SERVICE_ACCOUNT} \
    --location=${COMPOSER_LOCATION} \
    connections -- export ${COMPOSER_INSTANCE_DATA_FOLDER}/${CONNECTIONS_JSON_NAME}

# Copying the Data from the Composer environment to the backup bucket.
echo "Exporting Data..."
gsutil rsync -r -d ${COMPOSER_GCS_BUCKET_DATA_FOLDER} ${COMPOSER_BACKUP_BUCKET}/data

# Uncomment if pypi dependencies needed in environment.
# Getting the list of Python packages installed in the Composer environment and saving it to a file
# called requirements.txt.
echo "Exporting pypi packages..."
gcloud composer environments describe ${COMPOSER_NAME} \
    --impersonate-service-account=${DEV_SERVICE_ACCOUNT} \
    --location ${COMPOSER_LOCATION} \
    --format="value(config.softwareConfig.pypiPackages)" \
    > requirements_staging.txt

# Replacing the semicolon with a new line.
tr ';' '\n' < requirements_staging.txt > requirements.txt
# Replacing '====' with '=='
sed -i 's/===/==/g' requirements.txt
# Copying the requirements.txt file to the GCS bucket.
gsutil cp -r requirements.txt ${COMPOSER_BACKUP_BUCKET}

# Copying the DAGs from the Composer environment to the backup bucket.
echo "Exporting DAGs..."
gsutil rsync -r -d ${COMPOSER_GCS_BUCKET_DAGS_FOLDER} ${COMPOSER_BACKUP_BUCKET}/dags

# Deleting the Composer environment.
echo "Deleting ${COMPOSER_NAME} environment..."
gcloud composer environments delete ${COMPOSER_NAME} \
    --impersonate-service-account=${DEV_SERVICE_ACCOUNT} \
    --location=${COMPOSER_LOCATION} \
    --quiet

# Deleting the GCS bucket.
echo "Deleting ${COMPOSER_GCS_BUCKET} bucket..."
gsutil -m rm -r ${COMPOSER_GCS_BUCKET}

# Setting the project to the prod project.
gcloud config set project ${PROD_PROJECT_ID}
echo "Project set to ${PROD_PROJECT_ID}"

echo "Done"