CREATE OR REPLACE TABLE `{{ var.value.env_project }}.{{ params['trade_area_dataset'] }}.{{ params['functional_areas_30_table'] }}` -- table with the 30 trade areas
CLUSTER BY fk_sgplaces
AS
WITH
base_table AS (SELECT fk_sgplaces, SAFE.ST_GEOGFROMTEXT(geometry) AS polygon, coverage_target
FROM `{{ var.value.env_project }}.{{ params['trade_area_dataset'] }}.{{ params['processed_geometries_table'] }}` ), -->geometries table
exact_table AS (
SELECT
fk_sgplaces,
polygon
FROM base_table
WHERE coverage_target = 0.3 AND polygon IS NOT NULL), --change this if necessary 0.3, 0.5, 0.7



approx_table AS (
SELECT
fk_sgplaces,
ST_BUFFER(polygon, equivalent_radius*(SQRT(0.3/coverage_target)-1)) AS polygon --change this if necessary 0.3, 0.5, 0.7
FROM
(SELECT
fk_sgplaces,
polygon,
SQRT(ST_AREA(polygon)/ACOS(-1)) AS equivalent_radius,
coverage_target,
RANK() OVER (PARTITION BY fk_sgplaces ORDER BY coverage_target) AS coverage_rank
FROM base_table
WHERE fk_sgplaces NOT IN (SELECT fk_sgplaces FROM exact_table)
AND coverage_target IS NOT NULL AND polygon IS NOT NULL)
WHERE coverage_rank = 1)
SELECT
*
FROM exact_table
UNION ALL
SELECT
*
FROM approx_table