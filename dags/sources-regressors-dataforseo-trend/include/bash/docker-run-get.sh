sudo gsutil cp -r gs://{{ params['dataforseo-bucket'] }}/{{ params['ENDPOINT'] }}/ingestion_date={{ ds }}/trend-post-output.json /home/data/trend-post-output.json

sudo docker run --rm \
    --mount type=bind,source=/home/data,target=/app/data \
    --pull always {{ params['IMAGE'] }} \
    --endpoint {{ params['ENDPOINT'] }} \
    --method get

sudo gsutil cp -r /home/data/trend-get-output gs://{{ params['dataforseo-bucket'] }}/{{ params['ENDPOINT'] }}/ingestion_date={{ ds }}/trend-get-output

sudo rm -rf /home/data/trend-post-output.json
sudo rm -rf /home/data/trend-get-output