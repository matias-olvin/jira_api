ASSERT (
  SELECT COUNT(*)
  FROM `{{ params['bigquery-project'] }}.{{ params['bigquery-dataset'] }}.{{ params['bigquery-table'] }}`
) = (
  SELECT COUNT(*)
  FROM (
    SELECT DISTINCT
      {{ params['date-column'] }}
      , {{ params['fk-column'] }}
    FROM `{{ params['bigquery-project'] }}.{{ params['bigquery-dataset'] }}.{{ params['bigquery-table'] }}`
  )
)