--INSERT `{{ params['storage-prod'] }}.{{ params['cameo_staging_dataset'] }}.{{ params['cameo_visits_table'] }}`
--PARTITION BY local_date CLUSTER BY fk_sgplaces, CAMEO_USA AS
SELECT
fk_sgplaces,
local_date,
CAMEO_USA,
SUM(visit_score) AS visit_score
 FROM
(
  SELECT
  fk_sgplaces,
  CAMEO_USA,
  local_date,
  visit_score * weight AS visit_score

  FROM (
    SELECT * EXCEPT(local_date),
           DATE_TRUNC(local_date, MONTH) AS local_date
    FROM `{{ params['storage-prod'] }}.{{ params['visit_dataset'] }}.*`
    WHERE local_date >= "{{ params.date_start }}"
      AND local_date < "{{ params.date_end }}"
  )
  JOIN (
    SELECT device_id, local_date, zip_id
    FROM `{{ params['storage-prod'] }}.{{ params['device_zipcodes_v2_dataset'] }}.*`
    WHERE local_date >= "{{ params.date_start }}"
    AND local_date < "{{ params.date_end }}"
  )
  USING(device_id, local_date)
  JOIN(
    SELECT zip_id, CAMEO_USA, weight
    FROM {{ params['storage-prod'] }}.{{ params['static_demographics_dataset'] }}.{{ params['zipcode_demographics_table'] }}
  )
  USING(zip_id)
)
GROUP BY
fk_sgplaces,
local_date,
CAMEO_USA
HAVING(visit_score > 0)