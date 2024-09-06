CREATE OR REPLACE TABLE
 `{{ params['storage-prod'] }}.{{ params['visitor_brand_destinations_dataset'] }}.{{ params['observed_connections_table'] }}`
CLUSTER BY fk_sgplaces, fk_sgbrands
AS

SELECT *
FROM(
  SELECT fk_sgplaces, fk_sgbrands,
        sum(shared_devices) as shared_devices
  FROM `{{ params['storage-prod'] }}.{{ params['visitor_brand_destinations_monthly_dataset'] }}.*`
  WHERE local_date > DATE_TRUNC(DATE_SUB( CAST("2022-10-28" AS DATE), INTERVAL 12 MONTH), MONTH)
  GROUP BY fk_sgplaces, fk_sgbrands
)
LEFT JOIN (
  SELECT fk_sgplaces, sum(total_devices) as total_devices
  FROM (
    SELECT fk_sgplaces, local_date, any_value(total_devices) as total_devices
    FROM `{{ params['storage-prod'] }}.{{ params['visitor_brand_destinations_monthly_dataset'] }}.*`
    WHERE local_date > DATE_TRUNC(DATE_SUB( CAST("2022-10-28" AS DATE), INTERVAL 12 MONTH), MONTH)
    GROUP BY fk_sgplaces, local_date
  )
  GROUP BY fk_sgplaces
)
USING(fk_sgplaces)