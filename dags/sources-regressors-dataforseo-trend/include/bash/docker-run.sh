IMAGE="{{ params['IMAGE'] }}"
BUCKET="{{ params['dataforseo-bucket'] }}"
INPUT="{{ params['INPUT'] }}"
OUTPUT="{{ params['OUTPUT'] }}"
DATESTAMP="ingestion_date={{ ds }}"
ENDPOINT="{{ params['ENDPOINT'] }}"
METHOD="{{ params['METHOD'] }}"

sudo gsutil cp gs://$BUCKET/$ENDPOINT/$DATESTAMP/$INPUT /home/data/

sudo docker run --rm \
    --mount type=bind,source=/home/data,target=/app/data \
    --pull always $IMAGE \
    --endpoint $ENDPOINT \
    --method $METHOD

sudo gsutil cp -r /home/data/$OUTPUT gs://$BUCKET/$ENDPOINT/$DATESTAMP/$OUTPUT

sudo rm -rf /home/data/$INPUT
sudo rm -rf /home/data/$OUTPUT