-- Replace this by LOAD VISITS TABLE FROM PREVIOUS MONTHLY UPDATE STORED IN GCS
CREATE OR REPLACE TABLE
  `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['PlaceCompatibility_table'] }}` (fk_sgplaces STRING, compatible_versions STRING) AS
WITH
  get_visits_now AS (
    SELECT
      *
    FROM
      `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['SGPlaceDailyVisitsRaw_table'] }}`
  ),
  -- explode the visits
  daily_visits_now AS (
    SELECT
      DATE_ADD(local_date, INTERVAL ROW_NUMBER DAY) AS local_date,
      CAST(visits AS INT64) visits,
      fk_sgplaces,
      DATE_TRUNC(
        DATE_ADD(local_date, INTERVAL ROW_NUMBER DAY),
        WEEK(SUNDAY)
      ) AS week_start
    FROM
      (
        SELECT
          local_date,
          fk_sgplaces,
          JSON_EXTRACT_ARRAY(visits) AS visit_array, -- Get an array from the JSON string
        FROM
          get_visits_now
      )
      CROSS JOIN UNNEST (visit_array) AS visits -- Convert array elements to row
    WITH
    OFFSET
      AS ROW_NUMBER -- Get the position in the array as another column
    ORDER BY
      local_date,
      fk_sgplaces,
      ROW_NUMBER
  ),
  avg_visits_now AS (
    SELECT
      AVG(visits) AS avg_visits,
      fk_sgplaces
    FROM
      daily_visits_now
    WHERE
      (local_date < '2020-03-01')
      OR (
        local_date >= '2021-01-01'
        AND local_date <= CAST('{{ ds }}' AS DATE)
      )
    GROUP BY
      fk_sgplaces
  ),
  scaled_visits_now AS (
    SELECT
      SAFE_DIVIDE(visits, avg_visits) AS scaled_visits,
      local_date,
      fk_sgplaces
    FROM
      daily_visits_now
      INNER JOIN avg_visits_now USING (fk_sgplaces)
  ),
  daily_visits_prev AS (
    SELECT
      DATE_ADD(local_date, INTERVAL ROW_NUMBER DAY) AS local_date,
      CAST(visits AS INT64) visits,
      fk_sgplaces,
      DATE_TRUNC(
        DATE_ADD(local_date, INTERVAL ROW_NUMBER DAY),
        WEEK(SUNDAY)
      ) AS week_start
    FROM
      (
        SELECT
          local_date,
          fk_sgplaces,
          JSON_EXTRACT_ARRAY(visits) AS visit_array, -- Get an array from the JSON string
        FROM
          `{{ var.value.env_project }}.{{ params['postgres_staging_dataset'] }}.{{ params['SGPlaceDailyVisitsRaw_table'] }}_prev`
      )
      CROSS JOIN UNNEST (visit_array) AS visits -- Convert array elements to row
    WITH
    OFFSET
      AS ROW_NUMBER -- Get the position in the array as another column
    ORDER BY
      local_date,
      fk_sgplaces,
      ROW_NUMBER
  ),
  avg_visits_prev AS (
    SELECT
      AVG(visits) AS avg_visits,
      fk_sgplaces
    FROM
      daily_visits_prev
    WHERE
      (local_date < '2020-03-01')
      OR (
        local_date >= '2021-01-01'
        AND local_date <= CAST('{{ ds }}' AS DATE)
      )
    GROUP BY
      fk_sgplaces
  ),
  scaled_visits_prev AS (
    SELECT
      SAFE_DIVIDE(visits, avg_visits) AS scaled_visits,
      local_date,
      fk_sgplaces
    FROM
      daily_visits_prev
      INNER JOIN avg_visits_prev USING (fk_sgplaces)
  ),
  compatibility_metrics AS (
    SELECT
      *
    FROM
      (
        SELECT
          CORR(visits_now, visits_prev) AS corr_,
          fk_sgplaces
        FROM
          (
            SELECT
              fk_sgplaces,
              local_date,
              scaled_visits_now.scaled_visits AS visits_now,
              scaled_visits_prev.scaled_visits AS visits_prev
            FROM
              scaled_visits_now
              INNER JOIN scaled_visits_prev USING (fk_sgplaces, local_date)
          )
        GROUP BY
          fk_sgplaces
      )
      INNER JOIN (
        SELECT
          fk_sgplaces,
          GREATEST(
            SAFE_DIVIDE(SUM(visits_prev), SUM(visits_now)),
            SAFE_DIVIDE(SUM(visits_now), SUM(visits_prev))
          ) - 1 AS ratio_diff
        FROM
          (
            SELECT
              fk_sgplaces,
              local_date,
              scaled_visits_now.scaled_visits AS visits_now,
              scaled_visits_prev.scaled_visits AS visits_prev
            FROM
              scaled_visits_now
              INNER JOIN scaled_visits_prev USING (fk_sgplaces, local_date)
            WHERE
              local_date >= CURRENT_DATE()
              AND local_date <= DATE_ADD(CURRENT_DATE(), INTERVAL 7 DAY)
          )
        GROUP BY
          fk_sgplaces
      ) USING (fk_sgplaces)
    ORDER BY
      ratio_diff
  ),
  current_compatibility AS (
    SELECT
      fk_sgplaces,
      compatible_versions
    FROM
      `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgplaceraw_table'] }}`
      INNER JOIN `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['PlaceCompatibility_table'] }}` ON pid = fk_sgplaces
  )
SELECT
  fk_sgplaces,
  CASE
    WHEN compatible_with_prev THEN CONCAT(
      SUBSTR(
        compatible_versions,
        1,
        LENGTH(compatible_versions) - 1
      ),
      ', ',
      version,
      ']'
    )
    ELSE CONCAT('[', version, ']')
  END AS compatible_versions
FROM
  (
    SELECT DISTINCT
      pid AS fk_sgplaces
    FROM
      `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgplaceraw_table'] }}`
  )
  LEFT JOIN (
    SELECT
      fk_sgplaces,
      TRUE AS compatible_with_prev
    FROM
      compatibility_metrics
    WHERE
      corr_ > 0.9
      AND ratio_diff < 0.05
  ) USING (fk_sgplaces)
  LEFT JOIN current_compatibility USING (fk_sgplaces)
  LEFT JOIN (
    SELECT
      id AS version
    FROM
      `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['Version_table'] }}`
    ORDER BY
      start_date DESC
    LIMIT
      1
  ) ON TRUE