CREATE OR REPLACE TABLE
  `{{ var.value.env_project }}.{{ params['postgres_mv_dataset'] }}.{{ params['cityzipcodesmonthlyvisits_table'] }}`
AS

WITH zipcodes_by_city AS(
  SELECT city, state_abbreviation, postal_code
  FROM(
    SELECT city, state_abbreviation, JSON_EXTRACT_ARRAY(zipcodes) AS zipcode_array
    FROM
      `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['cityraw_table'] }}`
  )
  CROSS JOIN UNNEST(zipcode_array) AS postal_code
),

places AS(
  SELECT sg.pid as fk_sgplaces, sb.tenant_type, z.city, z.postal_code, z.state_abbreviation as region, sb.pid as fk_sgbrands
  FROM zipcodes_by_city as z
  JOIN
      `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgplaceraw_table'] }}` sg
    USING(postal_code, city)
  JOIN
      `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgbrandraw_table'] }}` sb
    ON sb.pid = sg.fk_sgbrands
  INNER JOIN
      `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgplaceactivity_table'] }}` a
    ON a.fk_sgplaces = sg.pid
  WHERE a.activity IN ('active', 'limited_data')
)

SELECT p.tenant_type, p.region, p.city, p.postal_code as fk_zipcodes, m.local_date, COALESCE(SUM(m.visits),0) as visits
FROM places p
LEFT JOIN
  `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['SGPlaceMonthlyVisitsRaw_table'] }}` m
  USING(fk_sgplaces)
GROUP BY m.local_date, p.city, p.region, p.postal_code, p.tenant_type