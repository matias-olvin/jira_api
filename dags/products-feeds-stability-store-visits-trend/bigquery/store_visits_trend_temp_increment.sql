CREATE OR REPLACE TABLE `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['store_visits_trend_temp_table'] }}`
PARTITION BY month_starting
CLUSTER BY store_id
AS
select
  places.pid as store_id,
  places.name,
  brands.name as brand,
  places.street_address,
  places.city,
  places.region as state,
  places.postal_code as zip_code,
  monthly_visits.local_date as month_starting,
  LAST_DAY(monthly_visits.local_date) as month_ending,
  total_visits
from 
    `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['postgres_batch_dataset'] }}-{{ params['sgplaceraw_table'] }}-{{ var.value.data_feed_places_version.replace('.', '-') }}` as places
    -- `storage-prod-olvin-com.postgres_final.SGPlaceRaw` as places
join
    `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['postgres_batch_dataset'] }}-{{ params['sgbrandraw_table'] }}-{{ var.value.data_feed_places_version.replace('.', '-') }}`  as brands
    -- `storage-prod-olvin-com.postgres_final.SGBrandRaw`  as brands
    on places.fk_sgbrands = brands.pid
join(
    SELECT fk_sgplaces
    FROM
    `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['postgres_batch_dataset'] }}-{{ params['sgplaceactivity_table'] }}-{{ var.value.data_feed_data_version.replace('.', '-') }}` as brands
    -- `storage-prod-olvin-com.postgres_final.SGPlaceActivity`
    WHERE activity in('active', 'limited_data')
)
    ON places.pid = fk_sgplaces
join (
    select fk_sgplaces, local_date, sum(cast(day_visits as INT64)) as total_visits
    from
         `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['postgres_batch_dataset'] }}-{{ params['sgplacedailyvisitsraw_table'] }}-{{ var.value.data_feed_data_version.replace('.', '-') }}`
        -- `storage-prod-olvin-com.postgres_final.SGPlaceDailyVisitsRaw`
    cross join
        UNNEST(JSON_EXTRACT_ARRAY(visits)) AS day_visits
    group by
        fk_sgplaces,
        local_date
    ) as monthly_visits
    on places.pid = monthly_visits.fk_sgplaces
where
    monthly_visits.local_date >= DATE_TRUNC('{{ ds }}', MONTH)
    AND monthly_visits.local_date < DATE_TRUNC(DATE_ADD('{{ ds }}', INTERVAL 1 MONTH), MONTH)