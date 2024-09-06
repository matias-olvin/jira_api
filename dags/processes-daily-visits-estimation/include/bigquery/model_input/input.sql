DECLARE range_partition INT64 DEFAULT  1000; 

DECLARE date_to_update DATE;
SET date_to_update = DATE("{{ ti.xcom_pull(task_ids='local-date') }}");



CREATE OR REPLACE TABLE `{{ var.value.sns_project }}.{{ params['visits-estimation-supervised-dataset'] }}_{{ params['stage'] }}.{{ params['rt-supervised-input-processed-table'] }}`
PARTITION BY RANGE_BUCKET(batch_index, GENERATE_ARRAY(0, range_partition, 1))
AS (
  WITH og_table AS (
    SELECT * 
    FROM `{{ var.value.sns_project }}.{{ params['visits-estimation-supervised-dataset'] }}_{{ params['stage'] }}.{{ params['batch-supervised-input-processed-table'] }}`
    WHERE local_date = date_to_update
  )
  , adding_all_factor AS (
    SELECT * EXCEPT(tier_id)
    FROM og_table
    LEFT JOIN (
      SELECT
        fk_sgplaces
        , tier_id
      FROM `{{ var.value.sns_project }}.{{ params['visits-estimation-supervised-dataset'] }}_{{ params['stage'] }}.{{ params['class-pois-sns-business-table'] }}`
      WHERE tier = 4
    ) USING (fk_sgplaces)
    LEFT JOIN (
      SELECT
        local_date AS ds
        , tier_id
        , factor AS factor_all
      FROM `{{ var.value.sns_project }}.{{ params['visits-estimation-supervised-dataset'] }}_{{ params['stage'] }}.{{ params['rt-factors-tier-id-business-table'] }}`
    ) USING (ds, tier_id)
  )
  , adding_business_tier_2 AS (
    SELECT * EXCEPT(tier_id)
    FROM adding_all_factor
    LEFT JOIN (
      SELECT
        fk_sgplaces
        , tier_id
      FROM `{{ var.value.sns_project }}.{{ params['visits-estimation-supervised-dataset'] }}_{{ params['stage'] }}.{{ params['class-pois-sns-business-table'] }}`
      WHERE tier = 2
    ) USING (fk_sgplaces)
    LEFT JOIN (
      SELECT
        local_date AS ds
        , tier_id
        , factor AS factor_business_tier_2
      FROM `{{ var.value.sns_project }}.{{ params['visits-estimation-supervised-dataset'] }}_{{ params['stage'] }}.{{ params['rt-factors-tier-id-business-table'] }}`
    ) USING (ds, tier_id)
  )
  , adding_business_tier_1 AS (
    SELECT * EXCEPT(tier_id)
    FROM adding_business_tier_2
    LEFT JOIN (
      SELECT
        fk_sgplaces
        , tier_id
      FROM `{{ var.value.sns_project }}.{{ params['visits-estimation-supervised-dataset'] }}_{{ params['stage'] }}.{{ params['class-pois-sns-business-table'] }}`
      WHERE tier = 1
    ) USING (fk_sgplaces)
    LEFT JOIN (
      SELECT local_date AS ds, tier_id, factor AS factor_business_tier_1
      FROM `{{ var.value.sns_project }}.{{ params['visits-estimation-supervised-dataset'] }}_{{ params['stage'] }}.{{ params['rt-factors-tier-id-business-table'] }}`
    ) USING (ds, tier_id)
  )
  , adding_location_tier_3 AS (
    SELECT * EXCEPT(tier_id)
    FROM adding_business_tier_1
    LEFT JOIN (
      SELECT 
        fk_sgplaces
        , tier_id
      FROM `{{ var.value.sns_project }}.{{ params['visits-estimation-supervised-dataset'] }}_{{ params['stage'] }}.{{ params['class-pois-sns-location-table'] }}`
      WHERE tier = 3
    ) USING (fk_sgplaces)
    LEFT JOIN (
      SELECT
        local_date AS ds
        , tier_id
        , factor AS factor_location_tier_3
      FROM `{{ var.value.sns_project }}.{{ params['visits-estimation-supervised-dataset'] }}_{{ params['stage'] }}.{{ params['rt-factors-tier-id-location-table'] }}`
    ) USING (ds, tier_id)
  )
  , adding_location_tier_2 AS (
    SELECT * EXCEPT(tier_id)
    FROM adding_location_tier_3
    LEFT JOIN (
      SELECT
        fk_sgplaces
        , tier_id
      FROM `{{ var.value.sns_project }}.{{ params['visits-estimation-supervised-dataset'] }}_{{ params['stage'] }}.{{ params['class-pois-sns-location-table'] }}`
      WHERE tier = 2
    ) USING (fk_sgplaces)
    LEFT JOIN (
      SELECT 
        local_date AS ds
        , tier_id
        , factor AS factor_location_tier_2
      FROM `{{ var.value.sns_project }}.{{ params['visits-estimation-supervised-dataset'] }}_{{ params['stage'] }}.{{ params['rt-factors-tier-id-location-table'] }}`
    ) USING (ds, tier_id)
  )
  , adding_location_tier_1 AS (
    SELECT * EXCEPT(tier_id)
    FROM adding_location_tier_2
    LEFT JOIN (
      SELECT
        fk_sgplaces
        , tier_id
      FROM `{{ var.value.sns_project }}.{{ params['visits-estimation-supervised-dataset'] }}_{{ params['stage'] }}.{{ params['class-pois-sns-location-table'] }}`
      WHERE tier = 1
    ) USING (fk_sgplaces)
    LEFT JOIN (
      SELECT
        local_date AS ds
        , tier_id
        , factor AS factor_location_tier_1
      FROM `{{ var.value.sns_project }}.{{ params['visits-estimation-supervised-dataset'] }}_{{ params['stage'] }}.{{ params['rt-factors-tier-id-location-table'] }}`
    ) USING (ds, tier_id)
  )
  -- REPLACING THE RESIDUALS
  -- notice that when im on a tier 2 business poi and changing the class_1 by adding the factor 2... what im actually doing is modifying the class_2 (will be passed to class 1) with the factor 1
  -- this is different FROM modifying the class 1 visits with the factor 2, which we opted not to do it in case of missing factor.
  , adding_the_factor AS (
    SELECT 
      * EXCEPT (
        gt_visits_business_class_1_residual_log
        , gt_visits_business_class_2_residual_log
        , gt_visits_class_all_residual_log
        , gt_visits_location_class_1_residual_log
        , gt_visits_location_class_2_residual_log
        , gt_visits_location_class_3_residual_log
        , factor_all, factor_business_tier_2
        , factor_business_tier_1
        , factor_location_tier_3
        , factor_location_tier_2
        , factor_location_tier_1
      )
      , CASE 
        WHEN tier_location = 1
          THEN gt_visits_location_class_1_residual_log + log(factor_location_tier_1) 
        WHEN tier_location = 2
          THEN gt_visits_location_class_1_residual_log + log(factor_location_tier_2) 
        WHEN tier_location = 3
          THEN gt_visits_location_class_1_residual_log + log(factor_location_tier_3) 
        ELSE gt_visits_location_class_1_residual_log + log(factor_all)
        END AS gt_visits_location_class_1_residual_log
      , gt_visits_location_class_1_residual_log AS gt_visits_location_class_1_residual_log_old
      , CASE 
        WHEN tier_location = 1
          THEN gt_visits_location_class_2_residual_log + log(factor_location_tier_2) 
        WHEN tier_location = 2
          THEN gt_visits_location_class_2_residual_log + log(factor_location_tier_2) 
        WHEN tier_location = 3
          THEN gt_visits_location_class_2_residual_log + log(factor_location_tier_3) 
        ELSE gt_visits_location_class_2_residual_log + log(factor_all)
        END AS gt_visits_location_class_2_residual_log
      , gt_visits_location_class_2_residual_log AS gt_visits_location_class_2_residual_log_old
      , CASE 
        WHEN tier_location = 1
          THEN gt_visits_location_class_3_residual_log + log(factor_location_tier_3) 
        WHEN tier_location = 2
          THEN gt_visits_location_class_3_residual_log + log(factor_location_tier_3) 
        WHEN tier_location = 3
          THEN gt_visits_location_class_3_residual_log + log(factor_location_tier_3) 
        ELSE gt_visits_location_class_3_residual_log + log(factor_all)
        END AS gt_visits_location_class_3_residual_log
      , gt_visits_location_class_3_residual_log AS gt_visits_location_class_3_residual_log_old
      , CASE 
        WHEN tier_business = 1
          THEN gt_visits_business_class_1_residual_log + log(factor_business_tier_1) 
        WHEN tier_business = 2
          THEN gt_visits_business_class_1_residual_log + log(factor_business_tier_2) 
        ELSE gt_visits_business_class_1_residual_log + log(factor_all)
        END AS gt_visits_business_class_1_residual_log
      , gt_visits_business_class_1_residual_log AS gt_visits_business_class_1_residual_log_old
      , CASE 
        WHEN tier_business = 1
          THEN gt_visits_business_class_2_residual_log + log(factor_business_tier_2) 
        WHEN tier_business = 2
          THEN gt_visits_business_class_2_residual_log + log(factor_business_tier_2) 
        ELSE gt_visits_business_class_2_residual_log + log(factor_all)
        END AS gt_visits_business_class_2_residual_log
      , gt_visits_business_class_2_residual_log AS gt_visits_business_class_2_residual_log_old
      , gt_visits_class_all_residual_log + log(factor_all) AS gt_visits_class_all_residual_log
      , gt_visits_class_all_residual_log AS gt_visits_class_all_residual_log_old
    FROM adding_location_tier_1
  )
  -- see documentation
  , remove_nulls AS (
    SELECT 
      * EXCEPT (
        gt_visits_business_class_1_residual_log
        , gt_visits_business_class_2_residual_log
        , gt_visits_class_all_residual_log
        , gt_visits_location_class_1_residual_log
        , gt_visits_location_class_2_residual_log
        , gt_visits_location_class_3_residual_log
        , gt_visits_business_class_1_residual_log_old
        , gt_visits_business_class_2_residual_log_old
        , gt_visits_class_all_residual_log_old
        , gt_visits_location_class_1_residual_log_old
        , gt_visits_location_class_2_residual_log_old
        , gt_visits_location_class_3_residual_log_old  
      
      )
      , IFNULL(gt_visits_business_class_1_residual_log, gt_visits_business_class_1_residual_log_old) AS gt_visits_business_class_1_residual_log
      , IFNULL(gt_visits_business_class_2_residual_log, gt_visits_business_class_2_residual_log_old) AS gt_visits_business_class_2_residual_log
      , IFNULL(gt_visits_class_all_residual_log, gt_visits_class_all_residual_log_old) AS gt_visits_class_all_residual_log
      , IFNULL(gt_visits_location_class_1_residual_log, gt_visits_location_class_1_residual_log_old) AS gt_visits_location_class_1_residual_log
      , IFNULL(gt_visits_location_class_2_residual_log, gt_visits_location_class_2_residual_log_old) AS gt_visits_location_class_2_residual_log
      , IFNULL(gt_visits_location_class_3_residual_log, gt_visits_location_class_3_residual_log_old) AS gt_visits_location_class_3_residual_log
    FROM adding_the_factor
  )
  
  , test_table AS (
    SELECT *
    FROM
      remove_nulls
      , (SELECT count(*) AS input_count FROM og_table )
      , (SELECT count(*) AS output_count FROM remove_nulls)
  )
  
  SELECT * EXCEPT(input_count, output_count)
  FROM test_table
  WHERE IF(
    (input_count = output_count)
    , TRUE
    , ERROR(FORMAT("no same rows in batch than in real time  %d <> %d ", output_count, input_count))
  )
)