sudo gsutil cp -r gs://{{ params['dataforseo-bucket'] }}/{{ params['ENDPOINT'] }}/ingestion_date={{ ds }}/reviews-post-output/batch={{ '%012d' % dag_run.conf['batch-number'] }}.json /home/data/reviews-post-output.json

sudo docker run --rm \
    --mount type=bind,source=/home/data,target=/app/data \
    --pull always {{ params['IMAGE'] }} \
    --endpoint {{ params['ENDPOINT'] }} \
    --method get

sudo gsutil cp -r /home/data/reviews-get-output gs://{{ params['dataforseo-bucket'] }}/{{ params['ENDPOINT'] }}/ingestion_date={{ ds }}/reviews-get-output/batch={{ '%012d' % dag_run.conf['batch-number'] }}

sudo rm -rf /home/data/reviews-post-output.json
sudo rm -rf /home/data/reviews-get-output