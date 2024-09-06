CREATE OR REPLACE TABLE
`{{ var.value.storage_project_id }}.{{ dag_run.conf['site_name'] }}.{{ params['raw_visits_table'] }}`
PARTITION BY local_date CLUSTER BY device_id AS

WITH
target_geog AS (
    SELECT ST_DIFFERENCE(polygon_site, remove_site) AS polygon_site
    FROM
    (SELECT {{ dag_run.conf['select_geometry'] }} AS polygon_site,
    {{ dag_run.conf['remove_geometry'] }} AS remove_site)
)
SELECT
    device_id,
    lat_long_visit_point,
    duration,
    local_date,
    local_hour,
    1 AS visit_score
FROM
(SELECT
*
FROM
(SELECT
device_id, lat_long_visit_point, duration, local_date, local_hour
FROM `{{ var.value.storage_project_id }}.{{ params['device_clusters_dataset'] }}.*` WHERE
 local_date >= "{{ dag_run.conf['start_date_string'] }}" AND local_date < "{{ dag_run.conf['end_date_string'] }}"
AND ST_WITHIN(lat_long_visit_point, {{ dag_run.conf['select_geometry'] }})
) JOIN
target_geog
ON ST_CONTAINS(polygon_site, lat_long_visit_point));