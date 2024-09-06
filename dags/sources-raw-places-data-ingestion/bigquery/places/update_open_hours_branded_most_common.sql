-- Update info about open hours before creating week array
--      At week array creation we assign default timetable to POIs w/ null info in open_hours field
--      Instead of that, for branded POIs we will set a different default timetable based on most repeated value within the brand


CREATE OR REPLACE TABLE
    `{{ params['project'] }}.{{ params['places_data_dataset'] }}.{{ params['dynamic_places_table'] }}`
AS

with
brand_hours_raw
as(
  SELECT pid, fk_sgbrands as brand, open_hours
  FROM
    `{{ params['project'] }}.{{ params['places_data_dataset'] }}.{{ params['dynamic_places_table'] }}`
  where fk_sgbrands is not null
),

diff_openhours
as(
  select distinct brand, open_hours, count(pid) as OH_num
  from brand_hours_raw
  where open_hours is not null
  group by brand, open_hours
  order by brand
),

diff_brand
as(
  select distinct brand, count(pid) as brand_num
  from brand_hours_raw
  group by brand
  order by brand
),

diff_brand_notnull
as(
  select distinct brand, count(pid) as brand_not_null_num
  from brand_hours_raw
  where open_hours is not null
  group by brand
  order by brand
),

merged_table
as(
  select distinct diff_openhours.brand, open_hours, OH_num, brand_num,
                                        brand_num-brand_not_null_num as null_num,
                                        100*OH_num/brand_not_null_num as percent_OH,
                                        100*(1-brand_not_null_num/brand_num) as percent_null
  from diff_openhours
  left join diff_brand
  on diff_openhours.brand = diff_brand.brand
  left join diff_brand_notnull
  on diff_openhours.brand = diff_brand_notnull.brand
  order by brand, percent_OH desc
),

max_OH_per_brand
as (
  select distinct brand, max(percent_OH) as max_percent_OH
  from merged_table
  group by brand
  order by brand
),

MostCommon_OpenHour_perBrand
as (
  select brand, open_hours as OpeningHours,
         OH_num as stores_sharing_OpeningHours, brand_num as total_stores_of_brand,
         null_num as total_null, percent_null, percent_OH
  from (
      select merged_table.*, max_OH_per_brand.max_percent_OH, row_number() over (partition by merged_table.brand) row_num
      from max_OH_per_brand
      inner join merged_table
      on max_OH_per_brand.brand = merged_table.brand and
        max_OH_per_brand.max_percent_OH = merged_table.percent_OH
  )
  where row_num = 1
),

hours_to_correct -- table with open_hours to assign if we find a POI of the brand that has no info in that field
as(
  select brand, OpeningHours as open_hours_brand
  from MostCommon_OpenHour_perBrand
  where stores_sharing_OpeningHours >= 5 and
        (total_null < 100*stores_sharing_OpeningHours or (total_null < 2000*stores_sharing_OpeningHours and percent_OH>70) )
  order by total_null desc
)
-- Conditions: most common OH appears at least 5 times
--      If this represents > 70% of known opening info (we are more confident about it) apply it to nulls if they are less than 2k times known
--      If % is lower (less confidence) set extrapolation threshold to 100 times


SELECT sgplaces.* except(open_hours),
      (case when open_hours is not null
            then open_hours
            else open_hours_brand
            end) as open_hours
FROM
    `{{ params['project'] }}.{{ params['places_data_dataset'] }}.{{ params['dynamic_places_table'] }}` sgplaces
left join hours_to_correct
on sgplaces.fk_sgbrands = hours_to_correct.brand