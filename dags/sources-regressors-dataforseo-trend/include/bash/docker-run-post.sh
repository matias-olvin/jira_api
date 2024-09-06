sudo gsutil cp -r gs://{{ params['dataforseo-bucket'] }}/{{ params['ENDPOINT'] }}/ingestion_date={{ ds }}/trend-post-input/ /home/data

sudo docker run --rm \
    --mount type=bind,source=/home/data,target=/app/data \
    --pull always {{ params['IMAGE'] }} \
    --endpoint {{ params['ENDPOINT'] }} \
    --method post

sudo gsutil cp -r /home/data/trend-post-output.json gs://{{ params['dataforseo-bucket'] }}/{{ params['ENDPOINT'] }}/ingestion_date={{ ds }}/trend-post-output.json

sudo rm -rf /home/data/trend-post-input
sudo rm -rf /home/data/trend-post-output.json