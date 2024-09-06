gcloud compute instances create $INSTANCE \
    --project=$PROJECT \
    --zone=$ZONE \
    --machine-type=$MACHINE_TYPE \
    --scopes=$SCOPES \
    --create-disk=auto-delete=yes,boot=yes,device-name=$INSTANCE,image=projects/debian-cloud/global/images/debian-12-bookworm-v20240312,mode=rw,size=1000,type=projects/$PROJECT/zones/$ZONE/diskTypes/pd-ssd \
    --metadata=enable-oslogin=TRUE,startup-script='#! /bin/bash
    sudo apt-get update && sudo apt-get upgrade -y
    sudo apt-get -y install postgresql-client parallel'