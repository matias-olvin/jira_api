INSERT
 `{{ params['storage-prod'] }}.{{ params['cameo_staging_dataset'] }}.{{ params['cameo_ema_24_table'] }}`
WITH
source_table AS (
    SELECT *
    FROM `{{ params['storage-prod'] }}.{{ params['cameo_staging_dataset'] }}.{{ params['cameo_visits_table'] }}`
    WHERE local_date >= DATE_TRUNC("{{ ds }}", MONTH) AND local_date < DATE_TRUNC(DATE_ADD("{{ ds }}", INTERVAL 1 MONTH), MONTH)
),
reference_cameo AS (
    SELECT * FROM
    (SELECT DISTINCT CAMEO_USA
    FROM {{ params['storage-prod'] }}.{{ params['static_demographics_dataset'] }}.{{ params['cameo_categories_processed_table'] }})
    CROSS JOIN
    (SELECT DISTINCT fk_sgplaces
    FROM source_table)
),
base_scores AS (
SELECT
local_date,
CAMEO_USA,
fk_sgplaces,
SUM(visit_score) AS visit_score
FROM
source_table
GROUP BY local_date, CAMEO_USA, fk_sgplaces
),
final_table AS (
SELECT
CAMEO_USA,
fk_sgplaces,
DATE_TRUNC("{{ ds }}", MONTH) AS local_date,
SUM(
  IFNULL(visit_score, 0) * (2/(24+1))
  + IFNULL(weighted_visit_score, 0)*(1-2/(24+1))
)AS weighted_visit_score,
FROM
base_scores
FULL OUTER JOIN
(SELECT weighted_visit_score, CAMEO_USA, fk_sgplaces
FROM
 `{{ params['storage-prod'] }}.{{ params['cameo_staging_dataset'] }}.{{ params['cameo_ema_24_table'] }}`
WHERE local_date = DATE_TRUNC(DATE_SUB("{{ ds }}", INTERVAL 1 MONTH), MONTH)
 )
USING(CAMEO_USA, fk_sgplaces)
FULL OUTER JOIN
reference_cameo
USING(CAMEO_USA, fk_sgplaces)
GROUP BY CAMEO_USA, fk_sgplaces, local_date
)
SELECT CAMEO_USA, fk_sgplaces, local_date, weighted_visit_score
 FROM
  final_table