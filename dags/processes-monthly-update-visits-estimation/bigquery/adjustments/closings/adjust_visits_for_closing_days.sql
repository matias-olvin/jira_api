create
or replace table 
`{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['adjustments_closings_output'] }}`
PARTITION BY local_date
CLUSTER BY fk_sgplaces
AS 
WITH get_fk_sgplaces AS (
  SELECT distinct *
  FROM
    ((

    SELECT
      fk_sgplaces,
      identifier,
      local_date,
      closing_factor
    FROM
    `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['adjustments_closing_days_logic'] }}` 
      -- `storage-dev-olvin-com.visits_estimation.adjustments_closing_days_logic`
      JOIN 
      `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['poi_class_table'] }}` 
      --  `storage-dev-olvin-com.visits_estimation.poi_class`
      ON tier_id = tier_id_sns
      )

    UNION ALL

    (
      select 
          fk_sgplaces,
          identifier,
          local_date,
          true as closing_factor 
      from
          `{{ var.value.prod_project }}.{{ params['static_features_dataset'] }}.{{ params['manual_closing_brands_table'] }}`
      inner join 
        -- `storage-prod-olvin-com.events.holidays` 
         `{{ var.value.prod_project }}.{{ params['events_dataset'] }}.{{ params['holidays_collection_table'] }}`
        using (identifier)

      inner join 
      (select pid as fk_sgplaces, fk_sgbrands from `{{ var.value.prod_project }}.{{ params['postgres_dataset'] }}.{{ params['sgplaceraw_table'] }}`) using (fk_sgbrands)

      inner join 
        `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['poi_class_table'] }}` 
        --  `storage-dev-olvin-com.visits_estimation.poi_class`
      using (fk_sgplaces)

      where opened = False
    ))

),

-- removing identifiers, above defined as closed, which manually we say they should be open
get_fk_sgplaces_removing_manual_ones AS (
  SELECT 
    * except(opened)
  FROM (
    SELECT
      *
    FROM
      get_fk_sgplaces
    left join
      (
        select pid as fk_sgplaces, identifier, opened
        from
        `{{ var.value.prod_project }}.{{ params['postgres_dataset'] }}.{{ params['sgplaceraw_table'] }}`
        inner join
        `{{ var.value.prod_project }}.{{ params['static_features_dataset'] }}.{{ params['manual_closing_brands_table'] }}`
        using (fk_sgbrands)
        where opened = True
      )
    USING (fk_sgplaces, identifier)
  )
  WHERE
    opened IS NULL
),

adjust_previous_output_with_closing AS (
  SELECT
    *
  EXCEPT
(visits),
    CASE
      WHEN closing_factor = TRUE THEN 0
      ELSE visits
    END AS visits
  FROM
    get_fk_sgplaces_removing_manual_ones
    RIGHT JOIN 
    `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['adjustments_events_output'] }}` 
    -- `storage-dev-olvin-com.visits_estimation.adjustments_events_output`
    USING (local_date, fk_sgplaces)
),
row_counts AS (
  SELECT
    (
      SELECT
        COUNT(*)
      FROM
      -- `storage-dev-olvin-com.visits_estimation.adjustments_events_output`
        `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['adjustments_events_output'] }}`
    ) AS start_rows,
    (
      SELECT
        COUNT(*)
      FROM
      adjust_previous_output_with_closing
        
    ) AS end_rows
),
 check_totals_before_and_after AS (
  SELECT
    (
    SELECT
      ROUND(SUM(visits) )
    FROM
    -- `storage-dev-olvin-com.visits_estimation.adjustments_events_output`
      `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['adjustments_events_output'] }}`
    WHERE
      fk_sgplaces NOT IN (
      SELECT
        distinct fk_sgplaces
      FROM
        get_fk_sgplaces_removing_manual_ones) ) AS start_sum_visits,
    (
    SELECT
      ROUND(SUM(visits) )
    FROM
      adjust_previous_output_with_closing
    WHERE
      fk_sgplaces NOT IN (
      SELECT
        distinct fk_sgplaces
      FROM
        get_fk_sgplaces_removing_manual_ones)) AS end_sum_visits )
SELECT
  fk_sgplaces,
  local_date,
  visits
FROM
  adjust_previous_output_with_closing
WHERE
  (
    SELECT
      start_rows
    FROM
      row_counts
  ) = (
    SELECT
      end_rows
    FROM
      row_counts
  )
  AND
  (
    SELECT 
      start_sum_visits
    FROM  
      check_totals_before_and_after
  ) = (
     SELECT 
      end_sum_visits
    FROM  
      check_totals_before_and_after
  );