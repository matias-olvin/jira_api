sudo gsutil -m cp -r gs://{{ params['dataforseo-bucket'] }}/{{ params['ENDPOINT'] }}/ingestion_date={{ ds }}/trend-get-output /home/data/

sudo docker run --rm \
    --mount type=bind,source=/home/data,target=/app/data \
    --pull always {{ params['IMAGE'] }} \
    --endpoint {{ params['ENDPOINT'] }} \
    --method transform

sudo gsutil cp -r /home/data/trend-transform-output.csv gs://{{ params['dataforseo-bucket'] }}/{{ params['ENDPOINT'] }}/ingestion_date={{ ds }}/trend-transform-output.csv

sudo rm -rf /home/data/trend-get-output
sudo rm -rf /home/data/trend-transform-output.csv
