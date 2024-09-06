CREATE OR REPLACE TABLE
`{{ params['storage-prod'] }}.{{ params['postgres_dataset'] }}.{{ params['cameo_table'] }}`
PARTITION BY local_date CLUSTER BY fk_sgplaces
AS
WITH
final_table AS (
SELECT
* EXCEPT(weighted_visit_score),

CAST(ROUND(weighted_visit_score * IFNULL(new_ratio, 1)) AS INT64) AS weighted_visit_score
FROM
 (SELECT * FROM `{{ params['storage-prod'] }}.{{ params['cameo_staging_dataset'] }}.{{ params['smoothed_transition_table'] }}`
 WHERE fk_sgplaces IN (SELECT DISTINCT pid
 FROM `{{ params['storage-prod'] }}.{{ params['postgres_dataset'] }}.{{ params['places_table'] }}`)
 )
 JOIN (SELECT pid AS fk_sgplaces, fk_sgbrands,
 IFNULL(opening_date, DATE("2019-01-01")) AS opening_date,
  IFNULL(closing_date, DATE(DATE_TRUNC("{{ ds }}", MONTH))) AS closing_date,
 FROM `{{ params['storage-prod'] }}.{{ params['postgres_dataset'] }}.{{ params['places_table'] }}`)
 USING(fk_sgplaces)
 LEFT JOIN
 `{{ params['storage-prod'] }}.{{ params['ground_truth_volume_dataset'] }}.{{ params['brand_scaling_factors_table'] }}`
 USING(fk_sgbrands)
 WHERE local_date >= opening_date AND local_date <= closing_date
),
reference_dates AS (
  SELECT local_date, fk_sgplaces
  FROM UNNEST(GENERATE_DATE_ARRAY(DATE('2019-01-01'), DATE_SUB(DATE_TRUNC(DATE_ADD("{{ ds }}", INTERVAL 1 YEAR), YEAR), INTERVAL 1 MONTH), INTERVAL 1 MONTH)) AS local_date
  CROSS JOIN (SELECT DISTINCT pid AS fk_sgplaces
  FROM `{{ params['storage-prod'] }}.{{ params['postgres_dataset'] }}.{{ params['places_table'] }}`)
  )
SELECT
fk_sgplaces,
local_date,
{% raw %} CONCAT("[", REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(TO_JSON_STRING(ARRAY_AGG(cameo_scores ORDER BY local_date_org)), "\\", ""), "]", "}"), "[", "{"), '","', ","), '{"{', "{{"), '}"}', "}}"), "}}", "}"), "}}", "}"), "{{", "{"), "{{", "{"), "]") {% endraw %} AS cameo_scores
FROM
(SELECT local_date AS local_date_org,
DATE_TRUNC(local_date, YEAR) AS local_date,
fk_sgplaces,
IFNULL(cameo_scores,  "{}") AS cameo_scores
FROM
(SELECT
local_date,
  fk_sgplaces,
  REPLACE(REPLACE(REPLACE(TO_JSON_STRING(ARRAY_AGG(STRUCT(CAST(CAMEO_USA AS STRING) AS CAMEO_USA, weighted_visit_score AS visit_score))), ',"visit_score"', ''), '"CAMEO_USA":', ''), '},{', ',') AS cameo_scores,
   FROM final_table
   JOIN (SELECT pid AS fk_sgplaces
   FROM `{{ params['storage-prod'] }}.{{ params['postgres_dataset'] }}.{{ params['places_table'] }}`)
   USING(fk_sgplaces)
   WHERE weighted_visit_score > 0
   GROUP BY local_date, fk_sgplaces)
   RIGHT JOIN reference_dates USING(local_date, fk_sgplaces))
    GROUP BY fk_sgplaces, local_date
