DECLARE date_to_update DATE;
DECLARE max_gt_date_batch DATE;
DECLARE max_gt_date_batch_minus_5_months DATE;

SET date_to_update = DATE("{{ ti.xcom_pull(task_ids='local-date') }}");
SET max_gt_date_batch = (
  SELECT MAX(ds)
  FROM `{{ var.value.sns_project }}.{{ params['visits-estimation-supervised-dataset'] }}_{{ params['stage'] }}.{{ params['batch-gt-scaled-groups-business-table'] }}`
);
SET max_gt_date_batch_minus_5_months = DATE_SUB(max_gt_date_batch, INTERVAL 5 MONTH);

BEGIN CREATE TEMP TABLE factor_table AS (
  WITH rt_visits_estimation AS (
  SELECT *
  FROM `{{ var.value.sns_project }}.{{ params['visits-estimation-supervised-dataset'] }}_{{ params['stage'] }}.{{ params['rt-visits-estimation-algo-output-table'] }}`
)
, get_visits AS (
    SELECT * 
    FROM `{{ var.value.sns_project }}.{{ params['accessible-dataset'] }}.{{ params['postgres-batch-dataset'] }}-{{ params['sgplacedailyvisitsraw-table'] }}`
)
, daily_visits AS ( -- explode the visits
  SELECT
    DATE_ADD(local_date, INTERVAL row_number DAY) AS local_date
    , CAST(visits AS FLOAT64) visits
    , fk_sgplaces
  FROM (
    SELECT
      local_date
      , fk_sgplaces
      , JSON_EXTRACT_ARRAY(visits) AS visit_array -- Get an array from the JSON string
    FROM get_visits
  )
  CROSS JOIN
    UNNEST(visit_array) AS visits -- Convert array elements to row
    WITH OFFSET AS row_number -- Get the position in the array AS another column
  ORDER BY
    local_date
    , fk_sgplaces
    , row_number
)
, replacing_date_to_update AS ( -- replacing date of interest on postgres final
  SELECT
    local_date
    , fk_sgplaces
    , IFNULL(rt_visits_estimation.visits, daily_visits.visits) AS visits
  FROM daily_visits
  LEFT JOIN rt_visits_estimation
    USING (local_date, fk_sgplaces)
)
, brand_visits_postgres_final AS (
  SELECT
    tier_id
    , local_date
    , AVG(visits_scaled) AS group_visits_scaled_pf 
  FROM (
    SELECT
      fk_sgplaces
      , local_date
      , tier_id
      , visits / NULLIF(AVG(IF(local_date <= max_gt_date_batch, visits, NULL)) OVER (PARTITION BY fk_sgplaces), 0) AS visits_scaled 
    FROM replacing_date_to_update 
    INNER JOIN `{{ var.value.sns_project }}.{{ params['visits-estimation-supervised-dataset'] }}_{{ params['stage'] }}.{{ params['rt-groups-business-class-1-table'] }}`
      USING (fk_sgplaces)
  )
  GROUP BY 1,2
)
, rule_of_three AS (  -- we use last months for a better link history/forecasts
  SELECT
    *
    , (
      AVG(IF((local_date <= max_gt_date_batch) and (local_date >= max_gt_date_batch_minus_5_months),  group_visits_scaled_pf, null )) OVER (PARTITION BY tier_id) /
      AVG(IF((local_date <= max_gt_date_batch) and (local_date >= max_gt_date_batch_minus_5_months),  group_gt_scaled, null )) OVER (PARTITION BY tier_id) * group_gt_scaled
    ) AS group_gt_scaled_volume_adjusted
  FROM brand_visits_postgres_final
  INNER JOIN (
    SELECT
      tier_id
      , local_date
      , group_gt_scaled
    FROM `{{ var.value.sns_project }}.{{ params['visits-estimation-supervised-dataset'] }}_{{ params['stage'] }}.{{ params['rt-gt-scaled-groups-business-table'] }}`
  ) USING (tier_id, local_date)
  WHERE local_date >= max_gt_date_batch_minus_5_months
)


, filter_tier_ids as (
select distinct tier_id from
  ( 
    SELECT corr(group_visits_scaled_pf, group_gt_scaled) corr_, tier_id
    FROM (select * from rule_of_three where (local_date <= max_gt_date_batch) and (local_date >= max_gt_date_batch_minus_5_months))
    group by tier_id
  )
where corr_ > 0.99
)


, factor_table AS (
  SELECT
    tier_id
    , group_gt_scaled_volume_adjusted / NULLIF(group_visits_scaled_pf, 0) AS factor_adjustment
  FROM rule_of_three
  inner join filter_tier_ids using (tier_id)
  WHERE local_date = date_to_update
)
SELECT * 
FROM factor_table
); END;


CREATE OR REPLACE TABLE `{{ var.value.sns_project }}.{{ params['visits-estimation-supervised-dataset'] }}_{{ params['stage'] }}.{{ params['rt-visits-estimation-table'] }}` AS ( 
  WITH adding_factor AS (
    SELECT
      fk_sgplaces
      , local_date
      , CAST(visits * IFNULL(factor_adjustment, 1) AS INT64) AS visits
    FROM `{{ var.value.sns_project }}.{{ params['visits-estimation-supervised-dataset'] }}_{{ params['stage'] }}.{{ params['rt-visits-estimation-algo-output-table'] }}`
    LEFT JOIN `{{ var.value.sns_project }}.{{ params['visits-estimation-supervised-dataset'] }}_{{ params['stage'] }}.{{ params['rt-groups-business-class-1-table'] }}` 
      USING (fk_sgplaces)
    LEFT JOIN factor_table
      USING (tier_id)
  )
  , test_table AS (
    SELECT *
    FROM
      adding_factor
      , (SELECT COUNT(*) AS input_count FROM `{{ var.value.sns_project }}.{{ params['visits-estimation-supervised-dataset'] }}_{{ params['stage'] }}.{{ params['rt-visits-estimation-algo-output-table'] }}`)
      , (SELECT COUNT(*) AS output_count FROM adding_factor)
      , (SELECT COUNT(*) AS input_count_distinct FROM (SELECT distinct fk_sgplaces, local_date FROM `{{ var.value.sns_project }}.{{ params['visits-estimation-supervised-dataset'] }}_{{ params['stage'] }}.{{ params['rt-visits-estimation-algo-output-table'] }}`))
      , (SELECT COUNT(*) AS output_count_distinct FROM ( SELECT distinct fk_sgplaces, local_date FROM adding_factor))
      , (SELECT COUNT(*) AS output_count_nulls FROM adding_factor where visits IS NULL)
  )
  SELECT * EXCEPT(
    input_count
    , output_count
    , output_count_distinct
    , input_count_distinct
    , output_count_nulls
  )
  FROM test_table
  WHERE IF(
    (input_count = output_count) AND (input_count_distinct = output_count_distinct) AND (output_count_nulls = 0)
    , TRUE
    , ERROR(FORMAT(
      "no same rows in batch than in real time  %d <> %d , or no same distinct fk_sgplaces in batch than in real time  %d <> %d  or some values are null %d "
      , output_count
      , input_count
      , output_count_distinct
      , input_count_distinct
      , output_count_nulls
    ))
  )
);

-- DELETE BEFORE INSERTING
DELETE FROM
  `{{ var.value.sns_project }}.{{ params['accessible-by-olvin-dataset'] }}.{{ params['visits-estimation-metrics-dataset'] }}-{{ params['logging-rt-factors-final_brand_adjustment-table'] }}`
WHERE
  group_id = "business"
  AND stage = "{{ params['stage'] }}"
  AND local_date = date_to_update
  AND run_date = DATE("{{ ds }}")
;
INSERT INTO `{{ var.value.sns_project }}.{{ params['accessible-by-olvin-dataset'] }}.{{ params['visits-estimation-metrics-dataset'] }}-{{ params['logging-rt-factors-final_brand_adjustment-table'] }}`
SELECT
  tier_id
  , factor_adjustment
  , "business" AS group_id
  , "{{ params['stage'] }}" AS stage
  , date_to_update AS local_date
  , DATE("{{ ds }}") AS run_date
FROM 
  factor_table
;

-- DELETE BEFORE INSERTING
DELETE FROM `{{ var.value.sns_project }}.{{ params['accessible-by-olvin-dataset'] }}.{{ params['visits-estimation-metrics-dataset'] }}-{{ params['logging-rt-factors-final_brand_adjustment-distribution-table'] }}`
WHERE
  group_id = "business"
  AND stage = "{{ params['stage'] }}"
  AND local_date = date_to_update
  AND run_date = DATE("{{ ds }}")
;
INSERT INTO `{{ var.value.sns_project }}.{{ params['accessible-by-olvin-dataset'] }}.{{ params['visits-estimation-metrics-dataset'] }}-{{ params['logging-rt-factors-final_brand_adjustment-distribution-table'] }}`
SELECT DISTINCT
  PERCENTILE_CONT(factor_adjustment, 0.05) OVER () AS perc_05_factor
  , PERCENTILE_CONT(factor_adjustment, 0.2) OVER () AS perc_20_factor
  , PERCENTILE_CONT(factor_adjustment, 0.5) OVER () AS median_factor
  , PERCENTILE_CONT(factor_adjustment, 0.7) OVER () AS perc_70_factor
  , PERCENTILE_CONT(factor_adjustment, 0.95) OVER () AS perc_95_factor
  , "business" AS group_id
  , "{{ params['stage'] }}" AS stage
  , date_to_update AS local_date
  , DATE("{{ ds }}") AS run_date
FROM factor_table
;