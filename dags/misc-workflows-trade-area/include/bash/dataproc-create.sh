#!bin/bash

set -e


rm -rf $REPO

GIT_TOKEN=$(gcloud secrets versions access $GCP_AUTH_SECRET_VERSION --secret="$GCP_AUTH_SECRET_NAME" --project $SECRET_PROJECT)
echo "$GIT_TOKEN"
echo "Cloning repository..."
git clone https://$USERNAME:${GIT_TOKEN}@github.com/$USERNAME/$REPO.git --branch $BRANCH
cd $REPO
packages=$(awk '!/platform_machine|platform_system|sys_platform/ {print $1}' requirements.txt | sed 's/\.post[0-9]*//g' | paste -sd, -)
gcloud dataproc clusters create $INSTANCE \
  --enable-component-gateway \
  --region $REGION \
  --master-machine-type $MASTER_MACHINE_TYPE \
  --master-boot-disk-size $MASTER_BOOT_DISK_SIZE \
  --num-master-local-ssds $MASTER_NUM_LOCAL_SSDS \
  --master-local-ssd-interface NVME \
  --num-workers $WORKER_NUM \
  --worker-machine-type $WORKER_MACHINE_TYPE \
  --worker-boot-disk-size $WORKER_BOOT_DISK_SIZE \
  --num-worker-local-ssds $WORKER_NUM_LOCAL_SSDS \
  --worker-local-ssd-interface NVME \
  --image-version $IMAGE_VERSION \
  --properties "^#^dataproc:pip.packages=$packages#spark:spark.dataproc.enhanced.optimizer.enabled=true#spark:spark.dataproc.enhanced.execution.enabled=true#dataproc:dataproc.cluster.caching.enabled=true" \
  --optional-components JUPYTER \
  --max-idle $MAX_IDLE \
  --max-age $MAX_AGE \
  --project $PROJECT

echo "Dataproc Cluster created successfully."

# Clean up
echo "Cleaning up..."
cd ..
rm -rf $REPO
