ASSERT (
  SELECT COUNT({{ params['fk-column'] }}) AS cnt
  FROM `{{ params['bigquery-project'] }}.{{ params['bigquery-dataset'] }}.{{ params['bigquery-table'] }}` t1
  WHERE NOT EXISTS (
    SELECT 1
    FROM `{{ params['bigquery-project'] }}.{{ params['comparison-dataset'] }}.{{ params['comparison-table'] }}` t2
    WHERE t1.{{ params['fk-column'] }} = t2.pid
  )
) = 0