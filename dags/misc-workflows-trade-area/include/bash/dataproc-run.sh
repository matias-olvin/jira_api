#!bin/bash

set -e

gcloud dataproc jobs submit pyspark --cluster=$INSTANCE \
  --region=$REGION \
  $PY_MAIN_FILE \
  --py-files=$PY_DIST \
  -- --gcs_output_path=$GCS_OUTPUT_PATH \
  --gcs_input_path=$GCS_INPUT_PATH \
  --input_schema_bool=$INPUT_SCHEMA_BOOL \
  --output_compression_bool=$OUTPUT_COMPRESSION_BOOL \
  --output_compression=$OUTPUT_COMPRESSION \
  --append_mode=$APPEND_MODE \
  --project=$PROJECT