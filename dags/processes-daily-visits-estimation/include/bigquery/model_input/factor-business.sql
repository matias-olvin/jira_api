DECLARE date_to_update DATE;
SET date_to_update = DATE("{{ ti.xcom_pull(task_ids='local-date') }}");

BEGIN CREATE TEMP TABLE batch_prediction AS (
  SELECT *
  FROM `{{ var.value.sns_project }}.{{ params['visits-estimation-supervised-dataset'] }}_{{ params['stage'] }}.{{ params['group-business-gt-scaled-prediction-table'] }}`
); END;

CREATE OR REPLACE TABLE `{{ var.value.sns_project }}.{{ params['visits-estimation-supervised-dataset'] }}_{{ params['stage'] }}.{{ params['rt-factors-tier-id-business-table'] }}` AS (
  WITH max_gt_local_date_on_batch AS (
    SELECT MAX(ds) AS max_local_date_batch 
    FROM `{{ var.value.sns_project }}.{{ params['visits-estimation-supervised-dataset'] }}_{{ params['stage'] }}.{{ params['batch-gt-scaled-groups-business-table'] }}`
  )
  , daily_gt AS (
    SELECT *
    FROM `{{ var.value.sns_project }}.{{ params['visits-estimation-supervised-dataset'] }}_{{ params['stage'] }}.{{ params['rt-gt-scaled-groups-business-table'] }}`
  )
  , data_prep AS (
    SELECT
      *
      , IF(local_date > max_local_date_batch, TRUE, FALSE) forecast_bool
    FROM (
      SELECT
        local_date
        , tier_id
        , b.group_gt_scaled AS batch_y
        , rt.group_gt_scaled AS real_time_y
        , max_local_date_batch
      FROM batch_prediction b
      INNER JOIN daily_gt rt
        USING (tier_id, local_date)
      , max_gt_local_date_on_batch
    )
  )

  , corr_history AS (  -- test to understand if all looks good
    SELECT 
      CORR(batch_y, real_time_y) AS corr_
      , tier_id
    FROM data_prep
    WHERE  forecast_bool = False AND local_date >= '2021-01-01'
    GROUP BY tier_id
  )
  , filtering_tier_ids AS (  -- filtering based on history correlation
    SELECT *
    FROM  data_prep
    INNER JOIN (
      SELECT tier_id
      FROM corr_history
      WHERE corr_ > 0.97
    ) USING (tier_id)
  )
  , rule_of_three AS (  -- we use last months for a better link history/forecasts
    SELECT
      *
      , (
        AVG(IF(forecast_bool OR (local_date < DATE_SUB(max_local_date_batch, INTERVAL 135 DAY)), NULL, batch_y)) OVER (PARTITION BY tier_id) /
        AVG(IF(forecast_bool or (local_date < DATE_SUB(max_local_date_batch, INTERVAL 135 DAY)), null, real_time_y)) OVER (PARTITION BY tier_id) * real_time_y
      ) AS real_time_y_volume_adjusted
    FROM filtering_tier_ids
  )
  SELECT
    tier_id
    , factor
    , local_date 
  FROM (
    SELECT
      *
      , real_time_y_volume_adjusted / IF(batch_y > 0.01, batch_y, NULL) AS factor
    FROM rule_of_three
  )
  WHERE
    local_date = date_to_update
    AND factor IS NOT NULL
);

-- DELETE BEFORE INSERTING
DELETE FROM `{{ var.value.sns_project }}.{{ params['accessible-by-olvin-dataset'] }}.{{ params['visits-estimation-metrics-dataset'] }}-{{ params['logging-rt-factors-tier-id-missing-table'] }}`
WHERE
  group_id = "business"
  AND stage = "{{ params['stage'] }}"
  AND local_date = date_to_update
  AND run_date = DATE("{{ ds }}")
;
INSERT INTO `{{ var.value.sns_project }}.{{ params['accessible-by-olvin-dataset'] }}.{{ params['visits-estimation-metrics-dataset'] }}-{{ params['logging-rt-factors-tier-id-missing-table'] }}`
SELECT
  tier_id
  , "business" AS group_id
  , "{{ params['stage'] }}" AS stage
  , date_to_update AS local_date
  , DATE("{{ ds }}") AS run_date
FROM (
  SELECT *
  FROM (
    SELECT DISTINCT tier_id 
    FROM batch_prediction
  ) 
  LEFT JOIN (
    SELECT DISTINCT
      tier_id
      , 1 AS dummy
    FROM `{{ var.value.sns_project }}.{{ params['visits-estimation-supervised-dataset'] }}_{{ params['stage'] }}.{{ params['rt-factors-tier-id-business-table'] }}`
  ) USING (tier_id)
) WHERE dummy IS NULL
;

-- DELETE BEFORE INSERTING
DELETE FROM `{{ var.value.sns_project }}.{{ params['accessible-by-olvin-dataset'] }}.{{ params['visits-estimation-metrics-dataset'] }}-{{ params['logging-rt-factors-tier-id-factors-distribution-table'] }}`
WHERE
  group_id = "business"
  AND stage = "{{ params['stage'] }}"
  AND local_date = date_to_update
  AND run_date = DATE("{{ ds }}")
;
INSERT INTO `{{ var.value.sns_project }}.{{ params['accessible-by-olvin-dataset'] }}.{{ params['visits-estimation-metrics-dataset'] }}-{{ params['logging-rt-factors-tier-id-factors-distribution-table'] }}`
SELECT DISTINCT
  PERCENTILE_CONT(factor, 0.05) OVER () AS perc_05_factor
  , PERCENTILE_CONT(factor, 0.2) OVER () AS perc_20_factor
  , PERCENTILE_CONT(factor, 0.5) OVER () AS median_factor
  , PERCENTILE_CONT(factor, 0.7) OVER () AS perc_70_factor
  , PERCENTILE_CONT(factor, 0.95) OVER () AS perc_95_factor
  , "business" AS group_id
  , "{{ params['stage'] }}" AS stage
  , date_to_update AS local_date
  , DATE("{{ ds }}") AS run_date
FROM `{{ var.value.sns_project }}.{{ params['visits-estimation-supervised-dataset'] }}_{{ params['stage'] }}.{{ params['rt-factors-tier-id-business-table'] }}`
;