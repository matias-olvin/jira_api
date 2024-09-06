DELETE FROM
    `{{ var.value.env_project }}.{{ params['metrics_dataset'] }}.{{ params['brand_hourly_summary_table'] }}`
WHERE run_date=DATE('{{ds}}')
;

INSERT INTO
    `{{ var.value.env_project }}.{{ params['metrics_dataset'] }}.{{ params['brand_hourly_summary_table'] }}`

WITH

current_month_hourly_visits AS(
  SELECT *,
         CAST(DATE_DIFF(local_date, DATE_TRUNC(local_date, WEEK), DAY) AS INT64)*24+local_hour AS week_hour
  FROM
  `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['adjustments_output_hourly_table'] }}`
  WHERE local_date >= DATE('{{ds}}')
    AND local_date < DATE_ADD(DATE('{{ds}}'), INTERVAL 1 MONTH)
),

avg_visits_per_week_hour AS(
  SELECT fk_sgplaces, local_hour, week_hour, AVG(visits) AS visits
  FROM current_month_hourly_visits
  GROUP BY fk_sgplaces, local_hour, week_hour
),

curr_month_visits_with_brand AS(
  SELECT fk_sgbrands, local_hour, week_hour, visits
  FROM avg_visits_per_week_hour
  INNER JOIN(
    SELECT pid AS fk_sgplaces, fk_sgbrands
    FROM
      `{{ var.value.env_project }}.{{ params['postgres_dataset'] }}.{{ params['sgplaceraw_table'] }}`
  )
  USING(fk_sgplaces)
),

total_per_hour_of_week AS(
  SELECT fk_sgbrands, week_hour, local_hour, SUM(visits) AS total_visits, COUNTIF(visits>0) AS num_open
  FROM curr_month_visits_with_brand
  GROUP BY fk_sgbrands, week_hour, local_hour
)

SELECT DATE('{{ds}}') AS run_date, brand, fk_sgbrands,
       local_hour, week_hour, total_visits, num_open
FROM(
  SELECT name AS brand, pid AS fk_sgbrands
  FROM
    `{{ var.value.env_project }}.{{ params['postgres_dataset'] }}.{{ params['sgbrandraw_table'] }}`
)
LEFT JOIN total_per_hour_of_week
USING(fk_sgbrands)