  --join sns raw data to model input
CREATE OR REPLACE TABLE
`{{ var.value.sns_project }}.{{ params['visits_estimation_dataset'] }}_{{ step }}{{env}}.{{ params['model_input_table'] }}`
  -- `sns-vendor-olvin-poc.visits_estimation.model_input`
PARTITION BY
  ds
CLUSTER BY
group_id AS
WITH
  get_sns_pois_with_high_accuracy AS (
  SELECT
    DISTINCT fk_sgplaces
  FROM
  `{{ var.value.sns_project }}.{{ params['visits_estimation_dataset'] }}_{{ step }}{{env}}.{{ params['list_sns_pois_table'] }}`
    -- `sns-vendor-olvin-poc.visits_estimation_dev.list_sns_pois`
  
 ), prepare_sns_data AS (
  SELECT
    fk_sgplaces,
    sum(visits) as visits,
    local_date
  FROM
  `{{ var.value.sns_project }}.{{ params['sns_raw_dataset'] }}.{{ params['raw_traffic_formatted_table'] }}`
    -- `sns-vendor-olvin-poc.sns_raw_develop.raw_traffic_formatted`
  WHERE
    fk_sgplaces IN (
    SELECT
      fk_sgplaces
    FROM
      get_sns_pois_with_high_accuracy) 
  GROUP BY fk_sgplaces, local_date
      ),
  ------- WE ADD NEW TIME SERIES SO WE ALSO NEED TO REGRESSORS ETC. FOR THESE TIME SERIES
  join_to_model_input_to_get_regressors AS (
  SELECT
    * EXCEPT(fk_sgplaces),
    CONCAT(fk_sgplaces, '_gt') fk_sgplaces
  FROM
  `{{ var.value.sns_project }}.{{ params['visits_estimation_dataset'] }}_{{ step }}{{env}}.{{ params['model_input_olvin_table'] }}` a
    -- `sns-vendor-olvin-poc.visits_estimation_dev.model_input_olvin` a
  RIGHT JOIN
    --UNION ALL NOT A JOIN
    prepare_sns_data b
  USING
    (fk_sgplaces,
      local_date) ),
  join_original_data_to_groundtruth AS (
  SELECT
    fk_sgplaces,
    y,
    ds,
    christmas_factor,
    temperature_res,
    temperature,
    precip_intensity,
    group_factor
  FROM
    `{{ var.value.sns_project }}.{{ params['visits_estimation_dataset'] }}_{{ step }}{{env}}.{{ params['model_input_olvin_table'] }}`
    -- `sns-vendor-olvin-poc.visits_estimation_dev.model_input_olvin`
  UNION ALL
  SELECT
    fk_sgplaces,
    y,
    ds,
    christmas_factor,
    temperature_res,
    temperature,
    precip_intensity,
    group_factor
  FROM
    join_to_model_input_to_get_regressors ),
  actual_visits AS (
  SELECT
    *
  FROM
    join_original_data_to_groundtruth,
     `{{ var.value.sns_project }}.{{ params['visits_estimation_dataset'] }}_{{ step }}{{env}}.{{ params['max_historical_date_table'] }}`
    -- `sns-vendor-olvin-poc.visits_estimation_dev.max_historical_date`
  WHERE
    DATE(ds) <= max_historical_date ),
  forecast_visits AS (
  SELECT
    * EXCEPT(y),
    NULL AS y
  FROM
    join_original_data_to_groundtruth,
     `{{ var.value.sns_project }}.{{ params['visits_estimation_dataset'] }}_{{ step }}{{env}}.{{ params['max_historical_date_table'] }}`
    -- `sns-vendor-olvin-poc.visits_estimation_dev.max_historical_date`
  WHERE
  DATE(ds) > max_historical_date),
  add_nulls_for_forecast_dates AS (
  SELECT
    fk_sgplaces,
    y,
    ds,
    christmas_factor,
    temperature_res,
    temperature,
    precip_intensity,
    group_factor
  FROM
    actual_visits
  UNION ALL
  SELECT
    fk_sgplaces,
    y,
    ds,
    christmas_factor,
    temperature_res,
    temperature,
    precip_intensity,
    group_factor
  FROM
    forecast_visits ),
  add_groups AS (
  SELECT
    fk_sgplaces,
    y,
    ds,
    christmas_factor,
    temperature_res,
    temperature,
    precip_intensity,
    group_factor,
    group_id
  FROM (
    SELECT
      fk_sgplaces,
      group_id
    FROM
        `{{ var.value.sns_project }}.{{ params['visits_estimation_dataset'] }}_{{ step }}{{env}}.{{ params['grouping_id_table'] }}`
    --  `sns-vendor-olvin-poc.visits_estimation_dev.grouping_map` 
    UNION ALL
    SELECT
      CONCAT(fk_sgplaces, "_gt") fk_sgplaces,
      group_id
    FROM
    `{{ var.value.sns_project }}.{{ params['visits_estimation_dataset'] }}_{{ step }}{{env}}.{{ params['grouping_id_gt_table'] }}`
    -- `sns-vendor-olvin-poc.visits_estimation_dev.grouping_map_gt` 
    )
     
  INNER JOIN
    add_nulls_for_forecast_dates
  USING
    (fk_sgplaces) ),
  checking_no_duplicates AS (
  SELECT
    *,
    COUNT(DISTINCT TO_JSON_STRING((fk_sgplaces,
          ds,
          group_id))) OVER () AS count_distinct,
    COUNT(*) OVER () AS count_rows
  FROM
    add_groups ),
  filter_nulls AS (
  SELECT
    *
  FROM
    add_groups t
  ,
     `{{ var.value.sns_project }}.{{ params['visits_estimation_dataset'] }}_{{ step }}{{env}}.{{ params['max_historical_date_table'] }}`
    -- `sns-vendor-olvin-poc.visits_estimation_dev.max_historical_date`
  WHERE
    REGEXP_CONTAINS(TO_JSON_STRING(t), r':(?:null|"")[,}]')
    AND DATE(ds) <= max_historical_date ),
  checking_no_nulls AS (
  SELECT
    *
  FROM
    checking_no_duplicates,
    (
    SELECT
      COUNT(*) AS number_null_rows
    FROM
      filter_nulls) ),
check_input_equal_to_output as (
select * FROM checking_no_nulls, (SELECT count(*) as input_number_rows FROM  checking_no_nulls where RIGHT(fk_sgplaces, 3) != "_gt" ),
(select count(*) output_number_rows from
    -- `sns-vendor-olvin-poc.visits_estimation.model_input_olvin`
     `{{ var.value.sns_project }}.{{ params['visits_estimation_dataset'] }}_{{ step }}{{env}}.{{ params['model_input_olvin_table'] }}`
)
),

check_sns_data_recent_enough as (
  select * from check_input_equal_to_output,
  (SELECT
    DATE_DIFF(MAX(CASE WHEN group_type = 'with_gt' THEN max_local_date END), MAX(CASE WHEN group_type = 'without_gt' THEN max_local_date END), DAY) date_diff_gt_olvin
  FROM
    (

        SELECT
          group_type,
          max_local_date,
          count,
          RANK() OVER (PARTITION BY group_type ORDER BY count DESC) AS rank
        FROM
          
          (
            SELECT
              max_local_date,
              COUNT(*) AS count,
              CASE
                WHEN fk_sgplaces LIKE '%_gt' THEN 'with_gt'
                ELSE 'without_gt'
              END AS group_type
            FROM
              (
                  SELECT
                    fk_sgplaces,
                    MAX(DATE(ds)) AS max_local_date
                  FROM
                    check_input_equal_to_output
                  where y is not null
                  GROUP BY
                    fk_sgplaces
              )
            GROUP BY
              max_local_date, group_type
          )

    )
  WHERE
    rank = 1
  )
)
SELECT
  * 
  EXCEPT (number_null_rows,
    count_distinct,
    count_rows,
    input_number_rows,output_number_rows,
    date_diff_gt_olvin
    )
FROM
  check_sns_data_recent_enough
WHERE
IF
  ( number_null_rows = 0
    AND (count_distinct = count_rows )
    AND (input_number_rows = output_number_rows)
    AND (date_diff_gt_olvin >= 0), TRUE, ERROR(FORMAT("there are %d null rows or number distinct fk_sgplaces, hour_ts (%d) is not equal to number of distinct rows (%d) or input_rows (%d) not equal to output rows minus _gt (%d) or date_diff_gt_olvin (%d) < 0 ", number_null_rows, count_distinct, count_rows, input_number_rows, output_number_rows, date_diff_gt_olvin))  );

DELETE FROM `{{ var.value.sns_project }}.{{ params['visits_estimation_dataset'] }}_{{ step }}{{env}}.{{ params['model_input_table'] }}`
WHERE fk_sgplaces IN(
SELECT fk_sgplaces
FROM `{{ var.value.sns_project }}.{{ params['visits_estimation_dataset'] }}_{{ step }}{{env}}.{{ params['model_input_table'] }}`
GROUP BY fk_sgplaces
HAVING sum(y) < 0.00001);