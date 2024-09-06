CREATE OR REPLACE TABLE `{{ var.value.env_project }}.{{ params['smc_ground_truth_volume_dataset'] }}.{{ params['median_ground_truth_category_visits'] }}`
AS
WITH 
remove_duplicates as (
  SELECT fk_sgplaces, visits_per_day AS ground_truth_visits
  FROM
--   `sns-vendor-olvin-poc.smc_gtvm.gtvm_target_agg_sns`
    `{{ params['sns_project'] }}.{{ params['accessible_by_olvin_dataset'] }}.v-{{ params['smc_gtvm_dataset'] }}-{{ params['gtvm_target_agg_sns_table'] }}`
),
join_naics_code as (
  SELECT a.*, b.naics_code FROM remove_duplicates a
  left join `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['places_dynamic_table'] }}` b
  on a.fk_sgplaces = b.pid
),
join_olvin_category as (
  SELECT a.*, b.olvin_category FROM join_naics_code a
  LEFT JOIN `{{ var.value.env_project }}.{{ params['sg_base_tables_dataset'] }}.{{ params['naics_code_subcategories_table'] }}` b
  using (naics_code)

),
get_median_visits_per_category as (
  SELECT distinct olvin_category,
  naics_code,
  PERCENTILE_CONT(ground_truth_visits,
    0.5) OVER (PARTITION BY olvin_category) AS olvin_category_median_visits
  FROM join_olvin_category 
)

select * from get_median_visits_per_category 