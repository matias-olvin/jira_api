CREATE OR REPLACE TABLE
 `{{ params['storage-prod'] }}.{{ params['cameo_staging_dataset'] }}.{{ params['cameo_hann_24_table'] }}`
PARTITION BY local_date CLUSTER BY fk_sgplaces, CAMEO_USA AS
WITH
source_table AS (
    SELECT *
    FROM `{{ params['storage-prod'] }}.{{ params['cameo_staging_dataset'] }}.{{ params['cameo_visits_table'] }}`
    WHERE local_date >= DATE_TRUNC("2018-01-01", MONTH) AND local_date < DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH), MONTH)
),
reference_dates AS (
    SELECT * FROM
    (SELECT DISTINCT local_date AS local_date_ref
    FROM source_table)
),
reference_cameo AS (
    SELECT * FROM
    (SELECT DISTINCT CAMEO_USA
    FROM {{ params['storage-prod'] }}.{{ params['static_demographics_dataset'] }}.{{ params['cameo_categories_processed_table'] }})
    CROSS JOIN
    (SELECT DISTINCT fk_sgplaces
    FROM source_table)
    CROSS JOIN
    reference_dates
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
month_factor_table AS (
SELECT
  local_date_ref,
  SUM(
  IF(ABS(DATE_DIFF(local_date_ref, local_date, MONTH)) <= 24,
0.5 + 0.5 * COS((DATE_DIFF(local_date_ref, local_date, MONTH)) * ACOS(-1) / (24)),
0)
)
 AS month_factor
FROM
(SELECT DISTINCT local_date FROM base_scores)
CROSS JOIN reference_dates
WHERE ABS(DATE_DIFF(local_date_ref, local_date, MONTH)) <= 24
--WHERE local_date <= local_date_ref
GROUP BY local_date_ref
)

SELECT
CAMEO_USA,
fk_sgplaces,
local_date_ref AS local_date,
IFNULL(weighted_visit_score/month_factor, 0) AS weighted_visit_score,
FROM
(SELECT
CAMEO_USA,
fk_sgplaces,
local_date_ref,
SUM(
IFNULL(visit_score, 0)
* IF(ABS(DATE_DIFF(local_date_ref, local_date, MONTH)) <= 24,
0.5 + 0.5 * COS((DATE_DIFF(local_date_ref, local_date, MONTH)) * ACOS(-1) / (24)),
0)
)
AS weighted_visit_score,
FROM
base_scores
CROSS JOIN
reference_dates
WHERE ABS(DATE_DIFF(local_date_ref, local_date, MONTH)) <= 24
--WHERE local_date <= local_date_ref
GROUP BY
CAMEO_USA,
fk_sgplaces,
local_date_ref)
JOIN
month_factor_table
USING(local_date_ref)
FULL OUTER JOIN
reference_cameo
USING(local_date_ref, CAMEO_USA, fk_sgplaces)