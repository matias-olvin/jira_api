CREATE OR REPLACE EXTERNAL TABLE `{{ params['project'] }}.{{ params['sg_places_dataset'] }}.{{ params['sg_places_reviews_table'] }}` 
  (
  fk_sgplaces STRING,
  total_reviews FLOAT64
)

WITH PARTITION COLUMNS (
version STRING, -- column order must match the external path
insert_date DATE)
OPTIONS (
format = 'JSON',
uris = ['gs://places-api-goog-olvin/reviews/*'],
hive_partition_uri_prefix = 'gs://{{ params["places_api_goog_data_bucket"] }}/{{ params["places_api_goog_reviews_folder"] }}',
require_hive_partition_filter = TRUE);