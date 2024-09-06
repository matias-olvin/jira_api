#!/bin/bash

set -exo pipefail

# Function to extract value after specified name and store it in a variable
extract_value() {
    local name="$1"
    local variable_name="$2"
    local value

    # Extract value after specified name
    value=$(awk -F "--name=$name --value=" '/--name='"$name"' --value=/ {print $2; exit}' "$CLIENT_KEYFILE.sh")

    # If value is found, export it as a variable
    if [ -n "$value" ]; then
        export "$variable_name"="$value"
    else
        echo "Error: $name value not found."
        exit 1
    fi
}

gcloud storage cp gs://$GCS_KEYS_BUCKET/$CLIENT_KEYFILE.sh .
#pby-product-p-gcs-euwe1-datafeed-keys/client-aws-send.sh .
# Check if unzip is installed, if not, install it
if ! command -v unzip &>/dev/null; then
    echo "unzip is not installed, installing..."
    sudo apt update
    sudo apt install -y unzip
fi

# Check if AWS CLI is installed, if not, download and install it
if ! command -v aws &>/dev/null; then
    echo "AWS CLI is not installed, downloading and installing..."
    curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
    unzip -o awscliv2.zip
    sudo ./aws/install
fi


# Extract access key
extract_value "fs.s3a.access.key" "access_key"

# Extract secret access key
extract_value "fs.s3a.secret.key" "secret_access_key"

# Output extracted values
echo "Access Key: $access_key"
echo "Secret Access Key: $secret_access_key"

# Export AWS access key ID and secret access key
export AWS_ACCESS_KEY_ID="$access_key"
export AWS_SECRET_ACCESS_KEY="$secret_access_key"

# Run AWS cp command
aws configure set default.s3.max_concurrent_requests 100
aws configure set default.s3.max_queue_size 10000
aws configure set default.s3.multipart_threshold 1024MB
aws configure set default.s3.multipart_chunksize 256MB
aws s3 cp s3://$S3_STAGING_BUCKET/$S3_STAGING_BUCKET_PREFIX s3://$S3_CLIENT_ACCESSPOINT_ARN/$S3_CLIENT_ACCESSPOINT_ARN_PREFIX --recursive
#s3://pby-public-data-feed-staging/twinbeech_capital s3://arn:aws:s3:us-east-1:173537357309:accesspoint/pby-accesspoint-test-ap/prefix
rm -rf $CLIENT_KEYFILE.sh