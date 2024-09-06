DELETE FROM
    `{{ var.value.env_project }}.{{ params['metrics_dataset'] }}.{{ params['brand_openings_closings_table'] }}`
WHERE run_date=DATE('{{ds}}')
;

INSERT INTO
    `{{ var.value.env_project }}.{{ params['metrics_dataset'] }}.{{ params['brand_openings_closings_table'] }}`

WITH

poi_monthly_visits AS (
  SELECT fk_sgplaces, local_date, SUM(visits) AS visits
  FROM(
    SELECT fk_sgplaces, DATE_TRUNC(local_date, MONTH) AS local_date, visits
    FROM
    `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['adjustments_places_dynamic_output_table'] }}`
  )
  GROUP BY fk_sgplaces, local_date
),

poi_brand_monthly_visits AS(
  SELECT fk_sgbrands, fk_sgplaces, local_date, visits
  FROM poi_monthly_visits
  INNER JOIN(
    SELECT pid AS fk_sgplaces, fk_sgbrands
    FROM
      `{{ var.value.env_project }}.{{ params['postgres_dataset'] }}.{{ params['sgplaceraw_table'] }}`
  )
  USING(fk_sgplaces)
),

info_from_visits AS(
  SELECT fk_sgbrands, local_date, COUNTIF(visits>0) AS num_pois_w_visits, SUM(visits) AS total_visits
  FROM poi_brand_monthly_visits
  GROUP BY fk_sgbrands, local_date
),

adjust_opening_info AS(
  SELECT pid AS fk_sgplaces, fk_sgbrands,
        IFNULL(b.actual_opening_date, a.opening_date) AS opening_date,
        IFNULL(b.actual_closing_date, a.closing_date) AS closing_date,
        IF(b.fk_sgplaces IS NULL, 0, 1) AS adjusted
  FROM
    `{{ var.value.env_project }}.{{ params['postgres_dataset'] }}.{{ params['sgplaceraw_table'] }}` a
  LEFT JOIN
    `{{ var.value.env_project }}.{{ params['places_dataset'] }}.{{ params['openings_adjustment_table'] }}` b
  ON pid = fk_sgplaces
),

dates_array AS(
  SELECT
    month
  FROM
    UNNEST(GENERATE_DATE_ARRAY('2019-01-01', DATE_ADD(DATE('{{ds}}'), INTERVAL 6 MONTH), INTERVAL 1 MONTH)) AS month
),

opening_status_table AS(
  SELECT fk_sgplaces, fk_sgbrands,
        month,
        CASE WHEN month >= IFNULL(opening_date, '2018-01-01')
              AND month < IFNULL(closing_date, '2100-01-01')
              THEN 1
              ELSE 0
        END AS is_open,
        adjusted
  FROM adjust_opening_info
  INNER JOIN dates_array
  ON TRUE
),

info_from_places AS(
  SELECT fk_sgbrands, month AS local_date, SUM(is_open) AS num_open_places_table, SUM(adjusted) AS num_adjusted
  FROM opening_status_table
  GROUP BY fk_sgbrands, month
)

SELECT DATE('{{ds}}') AS run_date, brand, fk_sgbrands, local_date,
       num_pois_w_visits, total_visits, num_open_places_table, num_adjusted
FROM(
  SELECT name AS brand, pid AS fk_sgbrands
  FROM
    `{{ var.value.env_project }}.{{ params['postgres_dataset'] }}.{{ params['sgbrandraw_table'] }}`
)
LEFT JOIN info_from_places
USING(fk_sgbrands)
LEFT JOIN info_from_visits
USING(fk_sgbrands, local_date)