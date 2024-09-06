CREATE OR REPLACE TABLE
  `{{ var.value.env_project }}.{{ params['manually_add_pois_dataset'] }}.{{ params['sgplacecameoraw_table'] }}`
PARTITION BY
  local_date
CLUSTER BY
  fk_sgplaces AS
WITH
  RECURSIVE date_sequence AS (
  SELECT
    DATE '2019-01-01' AS local_date
  UNION ALL
  SELECT
    DATE_ADD(local_date, INTERVAL 1 MONTH)
  FROM
    date_sequence
  WHERE
    DATE_ADD(local_date, INTERVAL 1 MONTH) <= DATE_TRUNC(CAST('{{ ds }}' AS DATE), MONTH) ),
  -- Reduce size of smoothed transition table and copy its data to previous months
  smoothed_transition AS (
  SELECT
    base_table.* EXCEPT (local_date),
    date_sequence.local_date
  FROM (
    SELECT
      *
    FROM
      `{{ var.value.env_project }}.{{ params['cameo_staging_dataset'] }}.{{ params['smoothed_transition_table'] }}`
    WHERE
      local_date >= DATE_SUB( DATE_TRUNC(CAST('{{ ds }}' AS DATE), MONTH), INTERVAL 1 YEAR )
      AND local_date < DATE_TRUNC(CAST('{{ ds }}' AS DATE), MONTH) ) base_table
  INNER JOIN
    date_sequence
  ON
    EXTRACT( MONTH
    FROM
      date_sequence.local_date ) = EXTRACT( MONTH
    FROM
      base_table.local_date ) ),
  -- Get info about nearby stores profile
  nearby_places AS (
  SELECT
    DISTINCT manual.pid AS manual_pid,
    manual.opening_date,
    place.pid
  FROM
    `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgplaceraw_table'] }}` place
  RIGHT JOIN
    `{{ var.value.env_project }}.{{ params['manually_add_pois_dataset'] }}.{{ params['sgplaceraw_table'] }}` manual
  ON
    ST_DWITHIN( ST_GEOGPOINT(place.longitude, place.latitude), ST_GEOGPOINT(manual.longitude, manual.latitude), 500 ) ),
  nearby_cameo_table AS (
  SELECT
    manual_pid,
    local_date,
    CAMEO_USA,
    weighted_visit_score
  FROM (
    SELECT
      manual_pid,
      smoothed_transition.*
    FROM
      smoothed_transition
    RIGHT JOIN
      nearby_places
    ON
      fk_sgplaces = pid )
  INNER JOIN (
    SELECT
      pid AS fk_sgplaces,
      fk_sgbrands,
      IFNULL(opening_date, DATE("2019-01-01")) AS opening_date,
      IFNULL( closing_date, DATE(DATE_TRUNC(CAST('{{ ds }}' AS DATE), MONTH)) ) AS closing_date
    FROM
      `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgplaceraw_table'] }}` )
  USING
    (fk_sgplaces)
  WHERE
    local_date >= opening_date
    AND local_date <= closing_date ),
  avg_nearby_profile AS (
  SELECT
    manual_pid,
    local_date,
    CAMEO_USA,
    IFNULL(SAFE_DIVIDE(sum_weight, total_weight), 0) AS perc_weight
  FROM (
    SELECT
      manual_pid,
      local_date,
      CAMEO_USA,
      SUM(weighted_visit_score) AS sum_weight
    FROM
      nearby_cameo_table
    GROUP BY
      manual_pid,
      local_date,
      CAMEO_USA )
  INNER JOIN (
    SELECT
      manual_pid,
      local_date,
      SUM(weighted_visit_score) AS total_weight
    FROM
      nearby_cameo_table
    GROUP BY
      manual_pid,
      local_date )
  USING
    (manual_pid,
      local_date) ),
  -- Get national average profile
  national_cameo_table AS (
  SELECT
    *
  FROM (
    SELECT
      smoothed_transition.*
    FROM
      smoothed_transition
    RIGHT JOIN
      `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgplaceraw_table'] }}`
    ON
      fk_sgplaces = pid )
  INNER JOIN (
    SELECT
      pid AS fk_sgplaces,
      fk_sgbrands,
      IFNULL(opening_date, DATE("2019-01-01")) AS opening_date,
      IFNULL( closing_date, DATE(DATE_TRUNC(CAST('{{ ds }}' AS DATE), MONTH)) ) AS closing_date
    FROM
      `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgplaceraw_table'] }}` )
  USING
    (fk_sgplaces)
  WHERE
    local_date >= opening_date
    AND local_date <= closing_date ),
  avg_national_profile AS (
  SELECT
    local_date,
    CAMEO_USA,
    IFNULL(SAFE_DIVIDE(sum_weight, total_weight), 0) AS perc_weight
  FROM (
    SELECT
      local_date,
      CAMEO_USA,
      SUM(weighted_visit_score) AS sum_weight
    FROM
      national_cameo_table
    GROUP BY
      local_date,
      CAMEO_USA )
  INNER JOIN (
    SELECT
      local_date,
      SUM(weighted_visit_score) AS total_weight
    FROM
      national_cameo_table
    GROUP BY
      local_date )
  USING
    (local_date) ),
  -- Get info about state avg profile
  same_state_places AS (
  SELECT
    DISTINCT manual.pid AS manual_pid,
    manual.opening_date,
    place.pid
  FROM
    `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgplaceraw_table'] }}` place
  RIGHT JOIN
    `{{ var.value.env_project }}.{{ params['manually_add_pois_dataset'] }}.{{ params['sgplaceraw_table'] }}` manual
  ON
    place.region = manual.region ),
  same_state_cameo_table AS (
  SELECT
    *
  FROM (
    SELECT
      manual_pid,
      smoothed_transition.*
    FROM
      smoothed_transition
    RIGHT JOIN
      same_state_places
    ON
      fk_sgplaces = pid )
  INNER JOIN (
    SELECT
      pid AS fk_sgplaces,
      fk_sgbrands,
      IFNULL(opening_date, DATE("2019-01-01")) AS opening_date,
      IFNULL( closing_date, DATE(DATE_TRUNC(CAST('{{ ds }}' AS DATE), MONTH)) ) AS closing_date
    FROM
      `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgplaceraw_table'] }}` )
  USING
    (fk_sgplaces)
  WHERE
    local_date >= opening_date
    AND local_date <= closing_date ),
  avg_state_profile AS (
  SELECT
    manual_pid,
    local_date,
    CAMEO_USA,
    IFNULL(SAFE_DIVIDE(sum_weight, total_weight), 0) AS perc_weight
  FROM (
    SELECT
      manual_pid,
      local_date,
      CAMEO_USA,
      SUM(weighted_visit_score) AS sum_weight
    FROM
      same_state_cameo_table
    GROUP BY
      manual_pid,
      local_date,
      CAMEO_USA )
  INNER JOIN (
    SELECT
      manual_pid,
      local_date,
      SUM(weighted_visit_score) AS total_weight
    FROM
      same_state_cameo_table
    GROUP BY
      manual_pid,
      local_date )
  USING
    (manual_pid,
      local_date) ),
  -- Get info about brand avg profiles
  same_brand_places AS (
  SELECT
    DISTINCT manual.pid AS manual_pid,
    manual.opening_date,
    place.pid
  FROM
    `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgplaceraw_table'] }}` place
  RIGHT JOIN
    `{{ var.value.env_project }}.{{ params['manually_add_pois_dataset'] }}.{{ params['sgplaceraw_table'] }}` manual
  ON
    place.fk_sgbrands = manual.fk_sgbrands ),
  same_brand_cameo_table AS (
  SELECT
    *
  FROM (
    SELECT
      manual_pid,
      smoothed_transition.*
    FROM
      smoothed_transition
    RIGHT JOIN
      same_brand_places
    ON
      fk_sgplaces = pid )
  INNER JOIN (
    SELECT
      pid AS fk_sgplaces,
      fk_sgbrands,
      IFNULL(opening_date, DATE("2019-01-01")) AS opening_date,
      IFNULL( closing_date, DATE(DATE_TRUNC(CAST('{{ ds }}' AS DATE), MONTH)) ) AS closing_date
    FROM
      `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgplaceraw_table'] }}` )
  USING
    (fk_sgplaces)
  WHERE
    local_date >= opening_date
    AND local_date <= closing_date ),
  avg_brand_profile AS (
  SELECT
    manual_pid,
    local_date,
    CAMEO_USA,
    IFNULL(SAFE_DIVIDE(sum_weight, total_weight), 0) AS perc_weight
  FROM (
    SELECT
      manual_pid,
      local_date,
      CAMEO_USA,
      SUM(weighted_visit_score) AS sum_weight
    FROM
      same_brand_cameo_table
    GROUP BY
      manual_pid,
      local_date,
      CAMEO_USA )
  INNER JOIN (
    SELECT
      manual_pid,
      local_date,
      SUM(weighted_visit_score) AS total_weight
    FROM
      same_brand_cameo_table
    GROUP BY
      manual_pid,
      local_date )
  USING
    (manual_pid,
      local_date) ),
  -- Get info about pois with same brand and state avg profile
  same_brand_state_places AS (
  SELECT
    DISTINCT manual.pid AS manual_pid,
    manual.opening_date,
    place.pid
  FROM
    `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgplaceraw_table'] }}` place
  RIGHT JOIN
    `{{ var.value.env_project }}.{{ params['manually_add_pois_dataset'] }}.{{ params['sgplaceraw_table'] }}` manual
  ON
    place.fk_sgbrands = manual.fk_sgbrands
    AND place.region = manual.region ),
  same_brand_state_cameo_table AS (
  SELECT
    *
  FROM (
    SELECT
      manual_pid,
      smoothed_transition.*
    FROM
      smoothed_transition
    RIGHT JOIN
      same_brand_state_places
    ON
      fk_sgplaces = pid )
  INNER JOIN (
    SELECT
      pid AS fk_sgplaces,
      fk_sgbrands,
      IFNULL(opening_date, DATE("2019-01-01")) AS opening_date,
      IFNULL( closing_date, DATE(DATE_TRUNC(CAST('{{ ds }}' AS DATE), MONTH)) ) AS closing_date
    FROM
      `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgplaceraw_table'] }}` )
  USING
    (fk_sgplaces)
  WHERE
    local_date >= opening_date
    AND local_date <= closing_date ),
  avg_brand_state_profile AS (
  SELECT
    manual_pid,
    local_date,
    CAMEO_USA,
    IFNULL(SAFE_DIVIDE(sum_weight, total_weight), 0) AS perc_weight
  FROM (
    SELECT
      manual_pid,
      local_date,
      CAMEO_USA,
      SUM(weighted_visit_score) AS sum_weight
    FROM
      same_brand_state_cameo_table
    GROUP BY
      manual_pid,
      local_date,
      CAMEO_USA )
  INNER JOIN (
    SELECT
      manual_pid,
      local_date,
      SUM(weighted_visit_score) AS total_weight
    FROM
      same_brand_state_cameo_table
    GROUP BY
      manual_pid,
      local_date )
  USING
    (manual_pid,
      local_date) ),
  -- GENERATE FINAL TABLE COMBINING PREVIOUS INFO
  final_table AS (
  SELECT
    manual_pid,
    local_date,
    CAMEO_USA,
    CASE
      WHEN num_brand_state >= 20 THEN CAST( GREATEST( 10000 * ( avg_nearby_profile.perc_weight - avg_state_profile.perc_weight + avg_brand_state_profile.perc_weight ), 0 ) AS INT64 )
    ELSE
    CAST( GREATEST( 10000 * ( avg_nearby_profile.perc_weight - avg_national_profile.perc_weight + avg_brand_profile.perc_weight ), 0 ) AS INT64 )
  END
    AS weighted_visit_score
  FROM
    avg_nearby_profile
  INNER JOIN
    avg_state_profile
  USING
    (manual_pid,
      local_date,
      CAMEO_USA)
  INNER JOIN
    avg_national_profile
  USING
    (local_date,
      CAMEO_USA)
  INNER JOIN
    avg_brand_state_profile
  USING
    (manual_pid,
      local_date,
      CAMEO_USA)
  INNER JOIN
    avg_brand_profile
  USING
    (manual_pid,
      local_date,
      CAMEO_USA)
  INNER JOIN (
    SELECT
      manual_pid,
      COUNT(*) AS num_brand_state
    FROM
      same_brand_state_cameo_table
    GROUP BY
      manual_pid )
  USING
    (manual_pid)
  INNER JOIN (
    SELECT
      pid AS manual_pid,
      IFNULL(opening_date, '2019-01-01') AS opening_date,
      IFNULL( closing_date, DATE_ADD( DATE_TRUNC(CAST('{{ ds }}' AS DATE), MONTH), INTERVAL 1 MONTH ) ) AS closing_date
    FROM
      `{{ var.value.env_project }}.{{ params['manually_add_pois_dataset'] }}.{{ params['sgplaceraw_table'] }}` )
  USING
    (manual_pid)
  WHERE
    local_date >= opening_date
    AND local_date <= closing_date ),
  -- Build the structure
  reference_dates AS (
  SELECT
    local_date,
    manual_pid
  FROM
    UNNEST ( GENERATE_DATE_ARRAY( DATE('2019-01-01'), DATE_SUB( DATE_TRUNC( DATE_ADD( DATE_TRUNC(CAST('{{ ds }}' AS DATE), MONTH), INTERVAL 1 YEAR ), YEAR ), INTERVAL 1 MONTH ), INTERVAL 1 MONTH ) ) AS local_date
  CROSS JOIN (
    SELECT
      DISTINCT pid AS manual_pid
    FROM
      `{{ var.value.env_project }}.{{ params['manually_add_pois_dataset'] }}.{{ params['sgplaceraw_table'] }}` ) ),
  -- Final table formatted
  SGPlaceCameoMonthlyRaw AS (
  SELECT
    manual_pid,
    local_date,
    {% raw %} CONCAT( "[", REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( TO_JSON_STRING( ARRAY_AGG( cameo_scores
                            ORDER BY
                              local_date_org ) ), "\\",
 "" ), "]", "}" ), "[", "{" ), '","', "," ), '{"{', "{{" ), '}"}', "}}" ), "}}", "}" ), "}}", "}" ), "{{", "{" ), "{{", "{" ), "]" ) AS cameo_scores
  FROM (
    SELECT
      manual_pid,
      local_date AS local_date_org,
      DATE_TRUNC(local_date, YEAR) AS local_date,
      IFNULL(cameo_scores, "{}") AS cameo_scores
    FROM (
      SELECT
        local_date,
        final_table.manual_pid AS manual_pid,
        REPLACE( REPLACE( REPLACE( TO_JSON_STRING( ARRAY_AGG( STRUCT ( CAST(CAMEO_USA AS STRING) AS CAMEO_USA,
                    weighted_visit_score AS visit_score ) ) ), ',"visit_score"', '' ), '"CAMEO_USA":', '' ), '},{', ',' ) AS cameo_scores {% endraw %}
      FROM
        final_table
      WHERE
        weighted_visit_score > 0
      GROUP BY
        local_date,
        final_table.manual_pid )
    RIGHT JOIN
      reference_dates
    USING
      (local_date,
        manual_pid) )
  GROUP BY
    manual_pid,
    local_date )
SELECT
  manual_pid AS fk_sgplaces,
  local_date,
  cameo_scores
FROM
  SGPlaceCameoMonthlyRaw