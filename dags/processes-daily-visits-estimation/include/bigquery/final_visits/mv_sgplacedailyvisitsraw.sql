CREATE OR REPLACE TABLE
  `{{ params['bigquery-project'] }}.{{ params['postgres-mv-rt-dataset'] }}.{{ params['sgplacedailyvisitsraw-table'] }}`
AS

SELECT v.*, fk_sgbrands, postal_code AS fk_zipcodes, region, city
FROM
  `{{ params['bigquery-project'] }}.{{ params['postgres-rt-dataset'] }}.{{ params['sgplacedailyvisitsraw-table'] }}` v
INNER JOIN(
  SELECT pid AS fk_sgplaces, fk_sgcenters, fk_sgbrands, city, region, postal_code, name
  FROM
      `{{ params['bigquery-project'] }}.{{ params['postgres-batch-dataset'] }}.{{ params['sgplaceraw-table'] }}`
  WHERE pid IN(
    SELECT fk_sgplaces
    FROM
      `{{ params['bigquery-project'] }}.{{ params['postgres-batch-dataset'] }}.{{ params['sgplaceactivity-table'] }}`
    WHERE activity IN('active', 'limited_data')
  )
)
USING(fk_sgplaces)
ORDER BY fk_sgbrands, fk_sgplaces, local_date
