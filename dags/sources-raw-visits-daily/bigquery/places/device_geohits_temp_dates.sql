WITH
  group_temp AS (
    SELECT local_date 
    FROM(
      SELECT 
        local_date,
        count(*) as row_count
      FROM 
        `{{ params['project'] }}.{{ params['device_geohits_staging_dataset'] }}.{{ staging_table }}`
      GROUP BY local_date
    )
    WHERE local_date > DATE_SUB(local_date, INTERVAL 20 DAY) 
  )

select local_date
from group_temp


