with 
drop_duplicates as (
  SELECT DISTINCT 
    sdk_ts, 
    event_id, 
    publisher_id, 
    country
  FROM  
    `{{ params['project'] }}.{{ params['raw_data_dataset'] }}.{{ execution_date.strftime('%Y') }}` 
),
aggregate_volume as (
  SELECT  
    EXTRACT(DATE FROM sdk_ts AT TIME ZONE 'America/Chicago'	) as chicago_local_date,
    DATE("{{ ds.format('%Y-%m-%d') }}") as provider_date,
    EXTRACT(DATE FROM TIMESTAMP(sdk_ts)) as gb_local_date, 
    count(*) as volume,
    country, 
    publisher_id
  FROM drop_duplicates 
  group by 
    gb_local_date, 
    chicago_local_date, 
    provider_date, 
    publisher_id,
    country
)

select 
  * EXCEPT(publisher_id), 
  CAST(publisher_id as INT64) as publisher_id 
from aggregate_volume
