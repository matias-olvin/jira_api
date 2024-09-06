#!/bin/bash
set -exo pipefail

# Check if azcopy is installed
if ! command -v azcopy &> /dev/null
then
    echo "azcopy is not found. Downloading..."
    wget -O azcopy.tar.gz https://aka.ms/downloadazcopy-v10-linux
    tar -xf azcopy.tar.gz --strip-components=1
    sudo mv azcopy /usr/bin/
fi

# Check if gcloud is installed
if ! command -v gcloud &> /dev/null
then
    echo "gcloud is not found. Installing..."
    curl https://sdk.cloud.google.com | bash
    exec -l $SHELL
fi

echo "Downloading service account keys from GCS to local..."
gcloud storage cp gs://$GCS_KEYS_BUCKET/$AZCOPY_CREDS_FILE_NAME.json . --project=data-feed-olvin

echo "Downloading .env file containing SAS token..."
gcloud storage cp gs://$GCS_KEYS_BUCKET/$CLIENT_TOKEN_TXT_FILE_NAME . --project=data-feed-olvin

token=$(cat $CLIENT_TOKEN_TXT_FILE_NAME)

# check if token is empty
if [ -z "$token" ]
then
    echo "Token is empty. Exiting..."
    exit 1
fi

export SAS_TOKEN=$token

# Create success file
touch _success_file

# Construct the blob URL
BLOB_URL="https://${ACCOUNT_NAME}.blob.core.windows.net/${CONTAINER_NAME}/${BLOB_NAME}?${SAS_TOKEN}"

# Use azcopy to upload the file
azcopy cp _success_file "${BLOB_URL}"

# Remove the success file
rm _success_file