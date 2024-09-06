CREATE OR REPLACE TABLE
`{{ params['storage-prod'] }}.{{ params['cameo_staging_dataset'] }}.{{ params['smoothed_transition_table'] }}`
PARTITION BY local_date CLUSTER BY fk_sgplaces, CAMEO_USA
AS
WITH
hann_table AS (
  SELECT * FROM
  `{{ params['storage-prod'] }}.{{ params['cameo_staging_dataset'] }}.{{ params['cameo_hann_24_table'] }}`
  WHERE local_date >= "2019-01-01" AND local_date < DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH), MONTH)
),
ema_table AS (
  SELECT * FROM
  `{{ params['storage-prod'] }}.{{ params['cameo_staging_dataset'] }}.{{ params['cameo_ema_24_table'] }}`
  WHERE local_date >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 24 MONTH), MONTH)
),
transition_table AS (
  SELECT fk_sgplaces, CAMEO_USA, local_date, ema.weighted_visit_score*ema.weight+hann.weighted_visit_score*hann.weight AS weighted_visit_score
  FROM (SELECT *, 1-DATE_DIFF(local_date, DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 13 MONTH), MONTH), MONTH)/12 AS weight
  FROM hann_table WHERE local_date >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 24 MONTH), MONTH)) AS hann
  JOIN (SELECT *, DATE_DIFF(local_date, DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 13 MONTH), MONTH), MONTH)/12 AS weight
   FROM ema_table WHERE local_date < DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH), MONTH)) AS ema
  USING(fk_sgplaces, CAMEO_USA, local_date)
),
final_table AS (
SELECT
fk_sgplaces, CAMEO_USA, local_date,
CAST(ROUND(weighted_visit_score) AS INT64) AS weighted_visit_score
FROM
(
  SELECT fk_sgplaces, CAMEO_USA, local_date, weighted_visit_score
  FROM hann_table
  WHERE local_date < DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 24 MONTH), MONTH)
  UNION ALL
  SELECT fk_sgplaces, CAMEO_USA, local_date, weighted_visit_score
  FROM ema_table
  WHERE local_date >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH), MONTH)
  UNION ALL
  SELECT fk_sgplaces, CAMEO_USA, local_date, weighted_visit_score
  FROM transition_table
  WHERE local_date >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 24 MONTH), MONTH) AND local_date < DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH), MONTH))
)
SELECT * FROM final_table