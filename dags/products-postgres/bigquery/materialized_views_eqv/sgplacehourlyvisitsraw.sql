CREATE OR REPLACE TABLE
  `{{ var.value.env_project }}.{{ params['postgres_mv_dataset'] }}.{{ params['SGPlaceHourlyVisitsRaw_table'] }}`
AS

SELECT v.*, fk_sgbrands, region, city
FROM
  `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['SGPlaceHourlyVisitsRaw_table'] }}` v
INNER JOIN(
  SELECT pid AS fk_sgplaces, fk_sgcenters, fk_sgbrands, city, region, postal_code, name
  FROM
      `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgplaceraw_table'] }}`
  WHERE pid IN(
    SELECT fk_sgplaces
    FROM
      `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgplaceactivity_table'] }}`
    WHERE activity IN('active', 'limited_data')
  )
)
USING(fk_sgplaces)
ORDER BY fk_sgbrands, fk_sgplaces, local_date