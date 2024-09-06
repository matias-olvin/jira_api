CREATE OR REPLACE TABLE
`{{ var.value.storage_project_id }}.{{ dag_run.conf['site_name'] }}.{{ params['trade_zipcodes_table'] }}_{{ source }}` AS
SELECT
fk_zipcodes,
primary_city,
ROUND({{ source }}_percentage/SUM({{ source }}_percentage) OVER ()*100, 2) AS {{ source }}_percentage,
polygon_zipcode,
FROM
(SELECT
fk_zipcodes,
SUM(visit_score) AS {{ source }}_percentage,
ANY_VALUE(polygon_zipcode) AS polygon_zipcode,
ANY_VALUE(primary_city) AS primary_city,
FROM `{{ var.value.storage_project_id }}.{{ dag_run.conf['site_name'] }}.{{ params['visitors_homes_table'] }}`
JOIN (SELECT pid AS fk_zipcodes, ST_GEOGFROMTEXT(polygon) AS polygon_zipcode, primary_city
FROM `{{ var.value.storage_project_id }}.{{ params['area_geometries_dataset'] }}.{{ params['zipcodes_table'] }}`)
ON ST_CONTAINS(polygon_zipcode, {{ source }}_point)
GROUP BY fk_zipcodes)

ORDER BY {{ source }}_percentage DESC