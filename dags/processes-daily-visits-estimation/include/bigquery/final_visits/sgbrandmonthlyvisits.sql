CREATE OR REPLACE TABLE `{{ params['bigquery-project'] }}.{{ params['postgres-mv-rt-dataset'] }}.{{ params['sgbrandmonthlyvisits-table'] }}` AS (
  SELECT
    fk_sgbrands
    , local_date
    , SUM(visits) AS visits
  FROM `{{ params['bigquery-project'] }}.{{ params['postgres-rt-dataset'] }}.{{ params['sgplacemonthlyvisitsraw-table'] }}`
  INNER JOIN(
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
);