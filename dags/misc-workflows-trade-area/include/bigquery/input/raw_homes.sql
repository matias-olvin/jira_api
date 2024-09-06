CREATE OR REPLACE TABLE `{{ var.value.env_project }}.{{ params['trade_area_dataset'] }}.{{ params['raw_homes_table'] }}`
CLUSTER BY fk_sgplaces, home_point
AS
WITH
poi_visits AS (
 SELECT fk_sgplaces, local_date, device_id,
  SUM(visit_score) AS visit_score
 FROM (
    SELECT
    fk_sgplaces, visit_score,
    DATE_TRUNC(local_date, MONTH) AS local_date,
    device_id
    FROM `{{ var.value.env_project }}.{{ params['poi_visits_scaled_dataset'] }}.*`
    WHERE local_date >= DATE_SUB(DATE("{{ ds.format('%Y-%m-01') }}"), INTERVAL 12 MONTH) AND local_date < DATE("{{ ds.format('%Y-%m-01') }}")
    AND fk_sgplaces IN (SELECT pid FROM `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgplaceraw_table'] }}`)
)
 GROUP BY fk_sgplaces, local_date, device_id
),

device_homes AS (
  SELECT
  local_date, device_id, home_point
  FROM `{{ var.value.env_project }}.{{ params['device_homes_dataset'] }}.*`
  WHERE home_point IS NOT NULL
  AND local_date >= DATE_SUB(DATE("{{ ds.format('%Y-%m-01') }}"), INTERVAL 12 MONTH) AND local_date < DATE("{{ ds.format('%Y-%m-01') }}")
),

raw_homes AS (SELECT
fk_sgplaces,
home_point,
visit_score
FROM poi_visits
JOIN device_homes
USING(device_id, local_date) 
WHERE visit_score>0.01),

useful_pois AS ( -- select only those with sufficient home points
  SELECT
  fk_sgplaces
  FROM raw_homes
  GROUP BY fk_sgplaces
  HAVING COUNT(*) > 5 AND COUNT(*) < 50000
)

SELECT
fk_sgplaces,
home_point,
visit_score
FROM raw_homes 
WHERE fk_sgplaces IN (SELECT * FROM useful_pois)