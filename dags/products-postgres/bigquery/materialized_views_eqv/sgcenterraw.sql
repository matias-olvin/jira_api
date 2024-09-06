CREATE OR REPLACE TABLE
  `{{ var.value.env_project }}.{{ params['postgres_mv_dataset'] }}.{{ params['SGCenterRaw_table'] }}`
AS

SELECT
  pid,
	name,
	naics_code,
	latitude,
	longitude,
	street_address,
	city,
	region,
	postal_code,
	category_tags,
	ST_ASTEXT(polygon_wkt) AS polygon,
	iso_country_code,
	polygon_area_sq_ft,
	opening_date,
	closing_date,
	opening_status,
	active_places_count,
	IFNULL(monthly_visits_availability, FALSE) AS monthly_visits_availability,
	IFNULL(patterns_availability, FALSE) AS patterns_availability,
--	ST_ASTEXT(point) AS point
FROM
  `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['SGCenterRaw_table'] }}`
WHERE
  pid IS NOT NULL
  AND
  name IS NOT NULL
  AND
  naics_code IS NOT NULL
  AND
  latitude IS NOT NULL
  AND
  longitude IS NOT NULL
  AND
  city IS NOT NULL
  AND
  region IS NOT NULL
  AND
  postal_code IS NOT NULL
  AND
  point IS NOT NULL
