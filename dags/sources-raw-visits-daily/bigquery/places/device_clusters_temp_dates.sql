
WITH
  group_temp AS (
    SELECT local_date 
    FROM(
      SELECT 
        local_date,
        count(*) as row_count
      FROM 
        `{{ params['project'] }}.{{ params['device_clusters_staging_dataset'] }}.{{ staging_table }}`
      GROUP BY local_date
    )
  )
  select local_date
  from group_temp
