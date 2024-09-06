DECLARE min_size_class_sns int64 DEFAULT 20;
DECLARE min_size_class_olvin int64 DEFAULT 20;


------------ 0.1  Tier Olvin ---------------------
BEGIN CREATE TEMP TABLE places_table as

with places_table as (
  select
    *,
  count(if(fk_sgbrands is not null, flag, null)) over (partition by fk_sgbrands) as count_brands,
  count(if(olvin_category is not null, flag, null)) over (partition by olvin_category) as count_categories,
  count(if(two_digit_code is not null, flag, null)) over (partition by two_digit_code) as count_two_digit_code,
    count(*) over () as count_all,

  from
  ( select pid as fk_sgplaces, naics_code, CAST(LEFT(CAST(naics_code AS STRING), 2) AS INT64) AS two_digit_code, fk_sgbrands
    from
    --  `storage-prod-olvin-com.postgres.SGPlaceRaw`
    `{{ var.value.prod_project }}.{{ params['postgres_dataset'] }}.{{ params['sgplaceraw_table'] }}` 
    where fk_sgbrands is not null
  )
  left join ( select naics_code, olvin_category from
    -- `storage-prod-olvin-com.sg_base_tables.naics_code_subcategories`
      `{{ var.value.prod_project }}.{{ params['sg_base_tables_dataset'] }}.{{ params['naics_code_subcategories_table'] }}` 
  ) using (naics_code)


  left JOIN
    (select distinct fk_sgplaces, 'flag' as flag from
    -- `storage-dev-olvin-com.visits_estimation.geospatial_output`
      `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['geospatial_output_table'] }}` 
    )
  using (fk_sgplaces)

)

select *
from places_table
;
end;


BEGIN CREATE TEMP TABLE places_olvin_tier_id as
with
places_tier_id as (
  select fk_sgplaces,
    CASE 
    WHEN fk_sgbrands is null then 3
    WHEN count_brands >= min_size_class_olvin then 1
    WHEN count_categories >= min_size_class_olvin then 2 
    WHEN count_two_digit_code >= min_size_class_olvin then 3
    ELSE 4
    end as tier,
    CASE 
    WHEN fk_sgbrands is null then 'all'
    WHEN count_brands >= min_size_class_olvin then fk_sgbrands
    WHEN count_categories >= min_size_class_olvin then olvin_category
    WHEN count_two_digit_code >= min_size_class_olvin then CONCAT('two_digit_code_',two_digit_code)
    ELSE 'all'
    end as tier_id
  from
    places_table

)
select *
from places_tier_id


;
end;


BEGIN CREATE TEMP TABLE class_pois_olvin as
with
class_pois_olvin as (
SELECT tier_id, fk_sgplaces ,
case 
when tier = 'tier_1_id' then 1
when tier = 'tier_2_id' then 2
when tier = 'tier_3_id' then 3
else 4
end as tier
FROM 
( 
  SELECT 
    fk_sgplaces,
    CASE 
    WHEN count_brands >= min_size_class_olvin then fk_sgbrands
    ELSE null
    end as tier_1_id,
    CASE 
    WHEN count_categories >= min_size_class_olvin then olvin_category
    ELSE null
    end as tier_2_id,
    CASE 
    WHEN count_two_digit_code >= min_size_class_olvin then CONCAT('two_digit_code_',two_digit_code)
    ELSE null
    end as tier_3_id,
    'all' as tier_4_id,

  FROM places_table
)
UNPIVOT(tier_id FOR tier IN (tier_1_id, tier_2_id, tier_3_id, tier_4_id ))
  
)
select *
from
class_pois_olvin

;
end;


------------ 0.1  Tier Olvin ---------------------
------------ 0.2  Tier SNS ---------------------

BEGIN CREATE TEMP TABLE places_table_sns as

with poi_prod_matching as (
  select
    *
  from
  ( select pid as fk_sgplaces, naics_code, CAST(LEFT(CAST(naics_code AS STRING), 2) AS INT64) AS two_digit_code, fk_sgbrands
    from
    --  `storage-prod-olvin-com.postgres.SGPlaceRaw`
    `{{ var.value.prod_project }}.{{ params['postgres_dataset'] }}.{{ params['sgplaceraw_table'] }}` 
    where fk_sgbrands is not null
  )

  left join ( select naics_code, olvin_category from
    -- `storage-prod-olvin-com.olvin_base.naics_code_subcategories`
      `{{ var.value.prod_project }}.{{ params['sg_base_tables_dataset'] }}.{{ params['naics_code_subcategories_table'] }}` 
  ) using (naics_code)


  left JOIN
    (select *, 'flag' as flag from
    -- `storage-prod-olvin-com.visits_estimation.list_sns_pois`
    `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['list_sns_pois_table'] }}` 
    )
  using (fk_sgplaces)

),


places_table_sns as (select *,
  count(if(fk_sgbrands is not null, flag, null)) over (partition by fk_sgbrands) as count_brands,
  count(if(olvin_category is not null, flag, null)) over (partition by olvin_category) as count_categories,
  count(if(two_digit_code is not null, flag, null)) over (partition by two_digit_code) as count_two_digit_code,
  count(flag) over () as count_all,
from poi_prod_matching)

select *
from places_table_sns;
end;


BEGIN CREATE TEMP TABLE places_sns_tier_id as
with
places_sns_tier_id as (
select fk_sgplaces,
  CASE 
  WHEN count_brands >= min_size_class_sns then 1
  WHEN count_categories >= min_size_class_sns then 2 
  WHEN count_two_digit_code >= min_size_class_olvin then 3
  ELSE 4
  end as tier,
  CASE 
  WHEN count_brands >= min_size_class_sns then fk_sgbrands
  WHEN count_categories >= min_size_class_sns then olvin_category
  WHEN count_two_digit_code >= min_size_class_olvin then CONCAT('two_digit_code_',two_digit_code)
  ELSE 'all'
  end as tier_id
from
  places_table_sns)

select *
from
places_sns_tier_id


;
end;



BEGIN CREATE TEMP TABLE class_pois_sns as
with
class_pois_sns as (
SELECT tier_id, fk_sgplaces ,
case 
when tier = 'tier_1_id' then 1
when tier = 'tier_2_id' then 2
when tier = 'tier_3_id' then 3
else 4
end as tier
FROM 
( 
  SELECT 
    fk_sgplaces,
    CASE 
    WHEN count_brands >= min_size_class_sns then fk_sgbrands
    ELSE null
    end as tier_1_id,
    CASE 
    WHEN count_categories >= min_size_class_sns then olvin_category
    ELSE null
    end as tier_2_id,
    CASE 
    WHEN count_two_digit_code >= min_size_class_sns then CONCAT('two_digit_code_',two_digit_code)
    ELSE null
    end as tier_3_id,
    'all' as tier_4_id,

  FROM places_table_sns
)
UNPIVOT(tier_id FOR tier IN (tier_1_id, tier_2_id, tier_3_id, tier_4_id ))
  
)
select *
from
class_pois_sns

;
end;


-- ------------ 0.2  Tier SNS ---------------------

-- ---- 0.3 TIER ---
BEGIN CREATE TEMP TABLE poi_class as

with poi_class_table as (
select 
fk_sgplaces,
a.tier as tier_olvin,
a.tier_id as tier_id_olvin,
ifnull(b.tier, 4) as tier_sns,
ifnull(b.tier_id, 'all') as tier_id_sns,
from
places_olvin_tier_id a
left join places_sns_tier_id b using (fk_sgplaces)
),
checking_consistency as
  (select
    *,
    count(if(tier_olvin = tier_sns, true, null) ) over() count_same_tier,
    count(if(tier_id_olvin = tier_id_sns, true, null) ) over () count_same_id
  from poi_class_table
  )

select * except(count_same_tier, count_same_id)
from checking_consistency

WHERE IF(
(count_same_tier = count_same_id) and (tier_olvin <= tier_sns),
TRUE,
ERROR(FORMAT("count_same_id  %d and count_same_tier %d are not the same. Or count_same_id  %d > tier_sns %d ", count_same_tier, count_same_id, tier_olvin, tier_sns))
)
; end;


BEGIN CREATE TEMP TABLE test_1 as
with test_1 as (
  select *
  from
  (
  select
    max(count_) over() - count_ + 1 as tier,
    count(*) as count_2
  from
  (SELECT fk_sgplaces, count(*) count_
  FROM class_pois_olvin
  group by fk_sgplaces)
  group by count_

  )

  inner join
  (SELECT tier_olvin as tier, count(*) count_1
  FROM poi_class
  group by tier_olvin)

  using (tier)
)
select *
from test_1
WHERE IF(
(count_2 = count_1),
TRUE,
ERROR(FORMAT("no consistency in test 1  %d <> %d ", count_1, count_2))
)

; end;




BEGIN CREATE TEMP TABLE test_2 as
with test_2 as (
  select *
  from
  (
  select
    max(count_) over() - count_ + 1 as tier,
    count(*) as count_2
  from
  (SELECT fk_sgplaces, count(*) count_
  FROM class_pois_sns
  group by fk_sgplaces)
  group by count_

  )

  inner join
  (SELECT tier_sns as tier, count(*) count_1
  FROM poi_class
  group by tier_sns)

  using (tier)
)
select *
from test_2
WHERE IF(
(count_2 = count_1),
TRUE,
ERROR(FORMAT("no consistency in test 2  %d <> %d ", count_1, count_2))
)

; end;


BEGIN CREATE TEMP TABLE test_3 as
with test_3 as (
  select *
  from
  (
  SELECT count(distinct pid) count_2
  FROM
    --  `storage-prod-olvin-com.postgres.SGPlaceRaw`
    `{{ var.value.prod_project }}.{{ params['postgres_dataset'] }}.{{ params['sgplaceraw_table'] }}` 
    where fk_sgbrands is not null
  )

  ,
  (SELECT count(*) count_1
  FROM poi_class
 )

)
select *
from test_3
WHERE IF(
(count_2 = count_1),
TRUE,
ERROR(FORMAT("no consistency in test 3  %d <> %d ", count_1, count_2))
)

; end;


BEGIN CREATE TEMP TABLE test_4 as
with test_4 as (
  select *
  from
  (
  SELECT count(*) count_2
  FROM
    (select distinct tier_id, fk_sgplaces from class_pois_sns)
  )
  ,
  (SELECT count(*) count_1
  FROM class_pois_sns
 )

)
select *
from test_4
WHERE IF(
(count_2 = count_1),
TRUE,
ERROR(FORMAT("no consistency in test 4  %d <> %d ", count_1, count_2))
)

; end;

BEGIN CREATE TEMP TABLE test_5 as
with test_5 as (
  select *
  from
  (
  SELECT count(*) count_2
  FROM
    (select distinct tier_id, fk_sgplaces from class_pois_olvin)
  )
  ,
  (SELECT count(*) count_1
  FROM class_pois_olvin
 )

)
select *
from test_5
WHERE IF(
(count_2 = count_1),
TRUE,
ERROR(FORMAT("no consistency in test 5  %d <> %d ", count_1, count_2))
)

; end;



create or replace table
  -- `storage-dev-olvin-com.visits_estimation.poi_class`
    `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['poi_class_table'] }}` 
as 
select *
from poi_class;


create or replace table
  -- `storage-dev-olvin-com.visits_estimation.class_pois_sns`
    `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['class_pois_sns_table'] }}` 
as 
select *
from class_pois_sns;


create or replace table
  -- `storage-dev-olvin-com.visits_estimation.class_pois_olvin`
    `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['class_pois_olvin_table'] }}` 
as 
select *
from class_pois_olvin;