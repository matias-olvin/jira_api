CREATE OR REPLACE TABLE `{{ var.value.env_project }}.{{ params['accessible_by_sns_dataset'] }}.{{ params['visits_daily_block_daily_estimation_table'] }}`
PARTITION BY local_date
CLUSTER BY fk_sgplaces
AS
WITH
metadata_table AS (
  SELECT * FROM
  `{{ var.value.env_project }}.{{ params['accessible_by_sns_dataset'] }}.smc_{{ params['sns_poi_metadata_table'] }}`
),
visits_table AS (
  SELECT visits_table.* FROM (
    SELECT
      fk_sgplaces, local_date,
      STRUCT(
        SUM(visit_score_steps.daily_estimation) AS daily_estimation
      ) AS visit_score_steps,
    FROM
      `{{ var.value.env_project }}.{{ params['poi_visits_block_daily_estimation_dataset'] }}.*`
    WHERE fk_sgplaces IN (
      SELECT DISTINCT fk_sgplaces
      FROM
        metadata_table
    )
    GROUP BY fk_sgplaces, local_date
  ) AS visits_table
),
all_dates_table AS (
  SELECT
    fk_sgplaces,
    local_date,
  FROM
    metadata_table
  JOIN (
    SELECT DISTINCT fk_sgplaces
    FROM visits_table
  ) USING (fk_sgplaces)
  CROSS JOIN
    UNNEST(GENERATE_DATE_ARRAY(min_date, max_date)) AS local_date
)

SELECT
  fk_sgplaces,
  local_date,
  STRUCT(
    IFNULL(visit_score_steps.daily_estimation, 0) AS daily_estimation
  ) AS visit_score_steps,
FROM
  all_dates_table
LEFT JOIN visits_table USING (fk_sgplaces, local_date)