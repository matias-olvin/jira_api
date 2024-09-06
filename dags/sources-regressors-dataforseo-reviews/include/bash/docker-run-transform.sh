sudo gsutil cp -r gs://{{ params['dataforseo-bucket'] }}/{{ params['ENDPOINT'] }}/ingestion_date={{ ds }}/reviews-get-output/batch={{ '%012d' % dag_run.conf['batch-number'] }} /home/data/
sudo mv /home/data/batch={{ '%012d' % dag_run.conf['batch-number'] }} /home/data/reviews-get-output

sudo docker run --rm \
    --mount type=bind,source=/home/data,target=/app/data \
    --pull always {{ params['IMAGE'] }} \
    --endpoint {{ params['ENDPOINT'] }} \
    --method transform

sudo gsutil cp -r /home/data/reviews-transform-output gs://{{ params['dataforseo-bucket'] }}/{{ params['ENDPOINT'] }}/ingestion_date={{ ds }}/reviews-transform-output/batch={{ '%012d' % dag_run.conf['batch-number'] }}

sudo rm -rf /home/data/reviews-get-output
sudo rm -rf /home/data/reviews-transform-output