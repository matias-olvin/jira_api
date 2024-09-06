CREATE OR REPLACE TABLE
`{{ var.value.storage_project_id }}.{{ dag_run.conf['site_name'] }}.{{ params['coordinates_table'] }}_{{ source }}` AS
SELECT
local_date,
local_hour,
duration,
visit_score,
max_local_date,
min_local_date,
ST_X({{ source }}_point) AS longitude,
ST_Y({{ source }}_point) AS latitude,
FROM
`{{ var.value.storage_project_id }}.{{ dag_run.conf['site_name'] }}.{{ params['visitors_homes_table'] }}`