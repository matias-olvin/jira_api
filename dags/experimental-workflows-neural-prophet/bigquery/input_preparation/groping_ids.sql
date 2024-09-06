DECLARE min_pois_brand int64 DEFAULT 100;

DECLARE min_pois_category int64 DEFAULT 500;

DECLARE max_elements_group int64 DEFAULT 1500;

-- we are randomising around max_elements_group, so buffer not to pass it
DECLARE buffer_value float64 DEFAULT 0.7;

BEGIN CREATE TEMP TABLE base as
select
  *,
  count(*) over (partition by fk_sgbrands) as pois_in_brand
from
  (
    select
      pid as fk_sgplaces,
      fk_sgbrands,
      naics_code,
      region,
      postal_code,
      latitude,
      longitude
    from
      `storage-prod-olvin-com.sg_places.places_dynamic`
    where
      fk_sgbrands is not null
  );

end;

-- dividing by brand size
BEGIN CREATE TEMP TABLE tier_big_brands as
select
  *
from
  (
    select
      *
    from
      base
    where
      pois_in_brand >= min_pois_brand
  );

end;

BEGIN CREATE TEMP TABLE tier_small_brands as
select
  *,
  count(*) over (partition by naics_code) as pois_in_category
from
  (
    select
      *
    from
      base
    where
      pois_in_brand < min_pois_brand
  );

end;

-- dividing by category
BEGIN CREATE TEMP TABLE tier_small_brands_big_category as
select
  *
from
  (
    select
      *
    from
      (
        select
          *
        from
          tier_small_brands
        where
          pois_in_category >= min_pois_category
      )
  );

end;

BEGIN CREATE TEMP TABLE tier_small_brands_small_category as
select
  *
from
  (
    select
      *,
      count(*) over (partition by region) as pois_in_region
    from
      (
        select
          *
        from
          tier_small_brands
        where
          pois_in_category < min_pois_category
      )
  );

end;

with grouping_strategy as (
  SELECT
    fk_sgplaces,
    case
      when count(*) over (partition by fk_sgbrands) >= max_elements_group then concat(
        fk_sgbrands,
        '_',
        cast(
          (
            rand() * floor(
              count(*) over (partition by fk_sgbrands) / (max_elements_group * buffer_value)
            )
          ) as int64
        )
      )
      else fk_sgbrands
    end as group_id
  FROM
    tier_big_brands
  union
  all
  SELECT
    fk_sgplaces,
    case
      when count(*) over (partition by naics_code) >= max_elements_group then concat(
        naics_code,
        '_',
        cast(
          (
            rand() * floor(
              count(*) over (partition by naics_code) / (max_elements_group * buffer_value)
            )
          ) as int64
        )
      )
      else cast(naics_code as string)
    end as group_id
  FROM
    tier_small_brands_big_category
  union
  all
  SELECT
    fk_sgplaces,
    case
      when count(*) over (partition by region) >= max_elements_group then concat(
        region,
        '_',
        cast(
          (
            rand() * floor(
              count(*) over (partition by region) / (max_elements_group * buffer_value)
            )
          ) as int64
        )
      )
      else region
    end as group_id
  FROM
    tier_small_brands_small_category
),
checking_consistent_size as (
  select
    *,
    count(*) over () as count_fk_sgplaces,
    count(distinct fk_sgplaces) over () as count_distinct_fk_sgplaces
  from
    grouping_strategy,
    (
      select
        count(*) as initial_count
      from
        base
    ),
    (
      select
        count(*) as final_count
      from
        grouping_strategy
    )
)
select
  *
except
  (
    count_fk_sgplaces,
    count_distinct_fk_sgplaces,
    initial_count,
    final_count
  )
from
  checking_consistent_size
WHERE
  IF(
    (count_fk_sgplaces = count_distinct_fk_sgplaces)
    and (initial_count = final_count),
    TRUE,
    ERROR(
      FORMAT(
        "count_distinct_fk_sgplaces  %d and count_fk_sgplaces %d are not the same. Or input %d and output table %d are not same size",
        count_fk_sgplaces,
        count_distinct_fk_sgplaces,
        initial_count,
        final_count
      )
    )
  )