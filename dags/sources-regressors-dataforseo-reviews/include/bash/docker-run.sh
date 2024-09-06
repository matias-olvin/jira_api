sudo gsutil cp -r gs://{{ params['dataforseo-bucket'] }}/{{ params['ENDPOINT'] }}/ingestion_date={{ ds }}/{{ params['INPUT'] }} /home/data/

sudo docker run --rm \
    --mount type=bind,source=/home/data,target=/app/data \
    --pull always {{ params['IMAGE'] }} \
    --endpoint {{ params['ENDPOINT'] }} \
    --method {{ params['METHOD'] }}

sudo gsutil cp -r /home/data/{{ params['OUTPUT'] }} gs://{{ params['dataforseo-bucket'] }}/{{ params['ENDPOINT'] }}/ingestion_date={{ ds }}/{{ params['OUTPUT'] }}

sudo rm -rf /home/data/{{ params['INPUT'] }}
sudo rm -rf /home/data/{{ params['OUTPUT'] }}
