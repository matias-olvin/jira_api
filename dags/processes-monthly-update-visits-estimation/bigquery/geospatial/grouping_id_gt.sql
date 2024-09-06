DECLARE min_pois_brand int64 DEFAULT 100;
DECLARE min_pois_category int64 DEFAULT 100;
DECLARE max_elements_group int64 DEFAULT 150;

CREATE OR REPLACE TABLE `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['grouping_id_gt_table'] }}` as
with sns_places as (
  SELECT
    DISTINCT fk_sgplaces
  FROM
  `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['list_sns_pois_table'] }}`
    -- `sns-vendor-olvin-poc.visits_estimation_dev.list_sns_pois`
),

grouping_strategy as (
select *
from  `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['grouping_id_table'] }}`
),

matching_group_of_sns as (
  select *,
  from sns_places
  left join grouping_strategy using (fk_sgplaces)
),


-- we force the group
tier_1_groups as (
  select *
  from matching_group_of_sns
  where group_id is not null
),

-- then groups by fk_sgbrands
grouping_sns_pois as (
  select *, row_number() over(partition by group_gt_id order by rand()) as row_id
  from 
  (
    select 
      fk_sgplaces,
      case
      when pois_in_brand >= min_pois_brand then fk_sgbrands
      when pois_in_category >= min_pois_category then  cast(naics_code as string)
      else region
      end as group_gt_id
    from
      (
        select
          *,
          count(*) over (partition by fk_sgbrands) as pois_in_brand,
          count(*) over (partition by naics_code) as pois_in_category,
          count(*) over (partition by region) as pois_in_region
        from tier_1_groups
        inner join 
          (select pid as fk_sgplaces, fk_sgbrands, naics_code, region, postal_code, latitude, longitude
          from `{{ var.value.prod_project }}.{{ params['places_dataset'] }}.{{ params['places_table'] }}`
          where fk_sgbrands is not null) using (fk_sgplaces)
      )
  )
),

final_table as (
  select fk_sgplaces, group_id, 2 as tier
  from (
    select distinct group_id 
    from grouping_strategy
    )
  left join (
    select * from grouping_sns_pois
    where row_id <= max_elements_group
  )
  on group_id like concat(group_gt_id, "%") 

)

select fk_sgplaces,group_id
from
  (select *, row_number() over(partition by fk_sgplaces , group_id order by tier asc) rd_numb
  from

    (select fk_sgplaces, group_id,  tier
    from final_table
    where fk_sgplaces is not null

    union all
    select fk_sgplaces, group_id, 1 as tier
    from tier_1_groups)
  )
where rd_numb =1