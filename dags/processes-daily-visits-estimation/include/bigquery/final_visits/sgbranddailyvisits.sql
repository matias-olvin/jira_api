CREATE OR REPLACE TABLE `{{ params['bigquery-project'] }}.{{ params['postgres-mv-rt-dataset'] }}.{{ params['sgbranddailyvisits-table'] }}` AS (
  WITH daily_visits as ( -- explode the visits
    SELECT
      DATE_ADD(local_date, INTERVAL row_number DAY) as local_date
      , CAST(visits as INT64) visits
      , fk_sgplaces
    FROM (
      SELECT
        local_date
        , fk_sgplaces
        , JSON_EXTRACT_ARRAY(visits) AS visit_array -- Get an array from the JSON string
      FROM `{{ params['bigquery-project'] }}.{{ params['postgres-rt-dataset'] }}.{{ params['sgplacedailyvisitsraw-table'] }}`
    )
    CROSS JOIN
      UNNEST(visit_array) AS visits -- Convert array elements to row
      WITH OFFSET AS row_number -- Get the position in the array as another column
    ORDER BY
      local_date
      , fk_sgplaces
      , row_number
  )
  , brand_agg_daily_visits AS (
    SELECT
      fk_sgbrands
      , local_date
      , SUM(visits) AS visits
    FROM daily_visits
    INNER JOIN (
      SELECT
        pid AS fk_sgplaces
        , fk_sgbrands
      FROM `{{ params['bigquery-project'] }}.{{ params['postgres-batch-dataset'] }}.{{ params['sgplaceraw-table'] }}`
      WHERE fk_sgbrands IN (
        SELECT pid
        FROM `{{ params['bigquery-project'] }}.{{ params['postgres-batch-dataset'] }}.{{ params['sgbrandraw-table'] }}`
      )
    ) USING(fk_sgplaces)
    WHERE fk_sgplaces IN (
      SELECT fk_sgplaces
      FROM `{{ params['bigquery-project'] }}.{{ params['postgres-batch-dataset'] }}.{{ params['sgplaceactivity-table'] }}`
      WHERE activity IN ('active', 'limited_data')
    )
    GROUP BY
      fk_sgbrands
      , local_date
  )
  SELECT
    fk_sgbrands
    , local_date
    , TO_JSON_STRING(ARRAY_AGG(IFNULL(visits, 0) ORDER BY local_date_sort)) AS visits
  FROM (
    SELECT
      fk_sgbrands
      , DATE_TRUNC(local_date, month) AS local_date
      , local_date AS local_date_sort
      , visits
    FROM brand_agg_daily_visits
  )
  GROUP BY
    fk_sgbrands
    , local_date
);