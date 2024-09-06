CREATE OR REPLACE TABLE
  `{{ var.value.env_project }}.{{ params['postgres_mv_dataset'] }}.{{ params['brandstatehourlyvisits_table'] }}`
AS

WITH
-- explode the visits
hourly_visits as (
  SELECT
    local_date,
    row_number AS local_hour,
    CAST(visits as INT64) visits,
    fk_sgplaces,
  FROM (
    SELECT
      local_date,
      fk_sgplaces,
      JSON_EXTRACT_ARRAY(visits) AS visit_array, -- Get an array from the JSON string
    FROM
      `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['SGPlaceHourlyVisitsRaw_table'] }}`
  )
  CROSS JOIN
    UNNEST(visit_array) AS visits -- Convert array elements to row
    WITH OFFSET AS row_number -- Get the position in the array as another column
  ORDER BY
    local_date,
    fk_sgplaces,
    row_number
),

brand_agg_hourly_visits AS(
  SELECT fk_sgbrands, state, local_date, local_hour, SUM(visits) AS visits
  FROM hourly_visits
  INNER JOIN(
    SELECT pid AS fk_sgplaces, fk_sgbrands, region AS state
    FROM
      `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgplaceraw_table'] }}`
    WHERE fk_sgbrands IN(
      SELECT pid
      FROM
        `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgbrandraw_table'] }}`
    )
  )
  USING(fk_sgplaces)
  WHERE fk_sgplaces IN(
    SELECT fk_sgplaces
    FROM
      `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgplaceactivity_table'] }}`
    WHERE activity IN('active', 'limited_data')
  )
  GROUP BY fk_sgbrands, state, local_date, local_hour
)


SELECT
  fk_sgbrands,
  state,
  local_date,
  TO_JSON_STRING(ARRAY_AGG( IFNULL(visits,
        0)
    ORDER BY
      local_hour )) AS visits
FROM brand_agg_hourly_visits
GROUP BY
  fk_sgbrands,
  state,
  local_date