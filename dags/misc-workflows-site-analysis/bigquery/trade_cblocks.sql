CREATE OR REPLACE TABLE
`{{ var.value.storage_project_id }}.{{ dag_run.conf['site_name'] }}.{{ params['trade_cblocks_table'] }}_{{ source }}` AS
SELECT
fk_cblocks,
ROUND({{ source }}_percentage/SUM({{ source }}_percentage) OVER ()*100, 2) AS {{ source }}_percentage,
polygon_block,
FROM
(SELECT
fk_cblocks,
SUM(visit_score) AS {{ source }}_percentage,
ANY_VALUE(polygon_block) AS polygon_block,
FROM `{{ var.value.storage_project_id }}.{{ dag_run.conf['site_name'] }}.{{ params['visitors_homes_table'] }}`
JOIN (SELECT GEOID20 AS fk_cblocks, geometry AS polygon_block,
FROM `{{ var.value.storage_project_id }}.{{ params['area_geometries_dataset'] }}.{{ params['cblocks_table'] }}`)
ON ST_CONTAINS(polygon_block, {{ source }}_point)
GROUP BY fk_cblocks)

ORDER BY {{ source }}_percentage DESC