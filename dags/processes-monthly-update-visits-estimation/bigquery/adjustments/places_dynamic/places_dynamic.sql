CREATE TEMP TABLE
  adjustments_dynamic_places_output_no_pad AS
WITH
  adjustments_volume_output AS (
    SELECT
      *
    FROM
      `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['adjustments_volume_output_table'] }}`
  ),
  poi_volume_restrictions AS (
    SELECT
      *
    FROM
      adjustments_volume_output
      INNER JOIN (
        SELECT
          pid AS fk_sgplaces,
          opening_date,
          closing_date
        FROM
          (
            SELECT
              *,
              COUNT(opening_status) OVER () AS count_final
            FROM
              (
                SELECT
                  og.* EXCEPT (closing_date, opening_date, opening_status),
                  COALESCE(adj.closing_date, og.closing_date) AS closing_date,
                  COALESCE(adj.opening_date, og.opening_date) AS opening_date,
                  COALESCE(adj.opening_status, og.opening_status) AS opening_status
                FROM
                  (
                    SELECT
                      *,
                      COUNT(opening_status) OVER () AS count_og
                    FROM
                      `{{ var.value.env_project }}.{{ params['postgres_dataset'] }}.{{ params['sgplaceraw_table'] }}`
                  ) og
                  LEFT JOIN (
                    SELECT
                      fk_sgplaces AS pid,
                      actual_closing_date AS closing_date,
                      actual_opening_date AS opening_date,
                      CASE
                        WHEN actual_closing_date IS NOT NULL THEN CASE
                          WHEN DATE_DIFF(
                            DATE_SUB(
                              DATE_ADD(
                                LAST_DAY(CAST('{{ ds }}' AS DATE)),
                                INTERVAL 1 DAY
                              ),
                              INTERVAL 1 MONTH
                            ),
                            DATE(actual_closing_date),
                            MONTH
                          ) >= 6 THEN 0
                          ELSE 2
                        END
                        ELSE 1
                      END AS opening_status
                    FROM
                      `{{ var.value.env_project }}.{{ params['places_dataset'] }}.{{ params['openings_adjustment_table'] }}`
                  ) adj USING (pid)
              )
          )
        WHERE
          IF(
            count_og = count_final,
            TRUE,
            ERROR(
              FORMAT(
                "count_og  %d and count_final %d are not the same.",
                count_og,
                count_final
              )
            )
          )
      ) USING (fk_sgplaces)
  )
SELECT
  * EXCEPT (opening_date, closing_date)
FROM
  poi_volume_restrictions
WHERE
  (
    (local_date >= opening_date)
    OR opening_date IS NULL
  )
  AND (
    (local_date < closing_date)
    OR closing_date IS NULL
  );

CREATE TEMP TABLE
  dates_to_pad AS
WITH
  openings_closings_dates AS (
    SELECT
      og.* EXCEPT (closing_date, opening_date, opening_status),
      COALESCE(adj.closing_date, og.closing_date) AS closing_date,
      COALESCE(adj.opening_date, og.opening_date) AS opening_date,
      COALESCE(adj.opening_status, og.opening_status) AS opening_status
    FROM
      (
        SELECT
          *,
          COUNT(opening_status) OVER () AS count_og
        FROM
          `{{ var.value.env_project }}.{{ params['postgres_dataset'] }}.{{ params['sgplaceraw_table'] }}`
      ) og
      LEFT JOIN (
        SELECT
          fk_sgplaces AS pid,
          actual_closing_date AS closing_date,
          actual_opening_date AS opening_date,
          CASE
            WHEN actual_closing_date IS NOT NULL THEN CASE
              WHEN DATE_DIFF(
                DATE_SUB(
                  DATE_ADD(
                    LAST_DAY(CAST('{{ ds }}' AS DATE)),
                    INTERVAL 1 DAY
                  ),
                  INTERVAL 1 MONTH
                ),
                DATE(actual_closing_date),
                MONTH
              ) >= 6 THEN 0
              ELSE 2
            END
            ELSE 1
          END AS opening_status
        FROM
          `{{ var.value.env_project }}.{{ params['places_dataset'] }}.{{ params['openings_adjustment_table'] }}`
      ) adj USING (pid)
  ),
  dates_to_pad_openings AS (
    SELECT
      fk_sgplaces,
      opening_month AS start_date,
      DATE_SUB(opening_date, INTERVAL 1 DAY) AS final_date
    FROM
      (
        SELECT
          pid AS fk_sgplaces,
          opening_date,
          DATE_TRUNC(opening_date, MONTH) AS opening_month
        FROM
          openings_closings_dates
        WHERE
          opening_date IS NOT NULL
          AND opening_date > '2019-01-01'
      )
    WHERE
      opening_date <> opening_month
  ),
  dates_to_pad_closings AS (
    SELECT
      fk_sgplaces,
      closing_date AS start_date,
      DATE_SUB(
        DATE_ADD(closing_month, INTERVAL 1 MONTH),
        INTERVAL 1 DAY
      ) final_date
    FROM
      (
        SELECT
          pid AS fk_sgplaces,
          closing_date,
          DATE_TRUNC(closing_date, MONTH) AS closing_month
        FROM
          openings_closings_dates
        WHERE
          closing_date IS NOT NULL
          AND closing_date > '2019-01-01'
      )
    WHERE
      closing_date <> closing_month
  )
SELECT
  *
FROM
  dates_to_pad_openings
UNION ALL
SELECT
  *
FROM
  dates_to_pad_closings
ORDER BY
  final_date;

CREATE TEMP TABLE
  dates_with_zero_visits_openings_closings AS
WITH RECURSIVE
  info_to_append AS (
    SELECT
      fk_sgplaces,
      start_date AS local_date,
      0 AS visits,
      final_date
    FROM
      dates_to_pad
    UNION ALL
    SELECT
      fk_sgplaces,
      DATE_ADD(local_date, INTERVAL 1 DAY) AS local_date,
      0 AS visits,
      final_date
    FROM
      info_to_append
    WHERE
      local_date < final_date
  )
SELECT
  * EXCEPT (final_date)
FROM
  info_to_append;

CREATE OR REPLACE TABLE
  `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['adjustments_places_dynamic_output'] }}`
PARTITION BY local_date
CLUSTER BY fk_sgplaces
AS
WITH
  adjustments_dynamic_places_output AS (
    SELECT
      *
    FROM
      adjustments_dynamic_places_output_no_pad
    UNION ALL
    SELECT
      *
    FROM
      dates_with_zero_visits_openings_closings
  ),
  tests AS (
    SELECT
      *
    FROM
      adjustments_dynamic_places_output,
      (
        SELECT
          COUNT(*) AS count_in
        FROM
          (
            SELECT DISTINCT
              fk_sgplaces,
              local_date
            FROM
              adjustments_dynamic_places_output
          )
      ),
      (
        SELECT
          COUNT(*) AS count_out
        FROM
          (
            SELECT
              fk_sgplaces,
              local_date
            FROM
              adjustments_dynamic_places_output
          )
      ),
      (
        SELECT
          CAST(SUM(visits) AS INT64) AS sum_in
        FROM
          `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['adjustments_volume_output_table'] }}`
        WHERE
          local_date = '2023-04-01'
      ),
      (
        SELECT
          CAST(SUM(visits) AS INT64) AS sum_out
        FROM
          adjustments_dynamic_places_output
        WHERE
          local_date = '2023-04-01'
      ),
      (
        SELECT
          COUNT(*) AS count_joined
        FROM
          (
            SELECT
              *
            FROM
              adjustments_dynamic_places_output
              INNER JOIN `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['adjustments_volume_output_table'] }}` USING (fk_sgplaces, local_date)
          )
      )
  )
SELECT
  * EXCEPT (
    count_in,
    count_out,
    sum_in,
    sum_out,
    count_joined
  )
FROM
  tests
WHERE
  IF(
    (count_in = count_out)
    AND (count_out = count_joined)
    AND (sum_in > sum_out),
    TRUE,
    ERROR(
      FORMAT(
        "count_in  %d <> count_out %d <> count_joined %d. or sum_out %d >= sum_in %d  ",
        count_in,
        count_out,
        count_joined,
        sum_in,
        sum_out
      )
    )
  )