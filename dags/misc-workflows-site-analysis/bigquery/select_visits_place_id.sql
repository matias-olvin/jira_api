CREATE OR REPLACE TABLE
`{{ var.value.storage_project_id }}.{{ dag_run.conf['site_name'] }}.{{ params['raw_visits_table'] }}`
PARTITION BY local_date CLUSTER BY device_id AS
WITH centroid_table AS (
    SELECT
        ST_UNION_AGG(ST_GEOGPOINT(longitude, latitude)) AS centroid
    FROM
    `{{ var.value.storage_project_id }}.sg_places.20211101`
    WHERE pid IN ({{ dag_run.conf['place_ids'] }})
)
SELECT
    device_id,
    lat_long_visit_point,
    duration,
    local_date,
    local_hour,
    visit_score,
FROM
(SELECT
*
FROM
(SELECT
device_id, lat_long_visit_point, duration, local_date, local_hour, visit_score
FROM `{{ var.value.storage_project_id }}.{{ params['poi_visits_scaled_dataset'] }}.*`, centroid_table
WHERE local_date >= "{{ dag_run.conf['start_date_string'] }}" AND local_date < "{{ dag_run.conf['end_date_string'] }}"
AND fk_sgplaces  IN ({{ dag_run.conf['place_ids'] }})
AND ST_DWITHIN(lat_long_visit_point, centroid, 200)
))