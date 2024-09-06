gcloud compute instances create $INSTANCE \
    --project=$PROJECT \
    --zone=$ZONE \
    --machine-type=$MACHINE_TYPE \
    --image-family=$IMAGE_FAMILY \
    --image-project=$IMAGE_PROJECT \
    --scopes=$SCOPES \
    --metadata enable-oslogin=TRUE