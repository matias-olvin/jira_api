sudo gsutil cp -r gs://{{ params['dataforseo-bucket'] }}/{{ params['ENDPOINT'] }}/ingestion_date={{ ds }}/reviews-post-input/batch={{ '%012d' % dag_run.conf['batch-number'] }} /home/data
sudo mv /home/data/batch={{ '%012d' % dag_run.conf['batch-number'] }} /home/data/reviews-post-input

sudo docker run --rm \
    --mount type=bind,source=/home/data,target=/app/data \
    --pull always {{ params['IMAGE'] }} \
    --endpoint {{ params['ENDPOINT'] }} \
    --method post

sudo gsutil cp -r /home/data/reviews-post-output.json gs://{{ params['dataforseo-bucket'] }}/{{ params['ENDPOINT'] }}/ingestion_date={{ ds }}/reviews-post-output/batch={{ '%012d' % dag_run.conf['batch-number'] }}.json

sudo rm -rf /home/data/reviews-post-input
sudo rm -rf /home/data/reviews-post-output.json