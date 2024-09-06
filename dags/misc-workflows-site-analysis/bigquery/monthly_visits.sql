
CREATE OR REPLACE TABLE
`{{ var.value.storage_project_id }}.{{ dag_run.conf['site_name'] }}.{{ params['monthly_visits_table'] }}` as

WITH 
 place_ids as (
SELECT
fk_sgplaces
-- FROM (select ['222-222@5pj-5nd-wkz','222-222@5pj-5nd-w8v','224-222@5pj-5nd-syv','zzw-222@5pj-5nd-syv','223-222@5pj-5nd-syv','224-222@5pj-5nd-snq','223-222@5pj-5nd-snq','222-223@5pj-5nd-snq']
FROM ( SELECT {{ dag_run.conf['place_ids'] }}
as fk_sgplaces),
UNNEST(fk_sgplaces) fk_sgplaces

),
get_visits as (
  SELECT * FROM place_ids a
  LEFT JOIN
  `{{ var.value.storage_project_id }}.{{  params['visits_estimation_dataset'] }}.{{ params['visits_estimation_table'] }}` b
--   `storage-prod-olvin-com.sg_visits_estimation_output.poi_daily` b 
  using(fk_sgplaces)
  WHERE b.local_date >= DATE_SUB(DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY), INTERVAL 12 MONTH) and local_date <= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY)

),
explode_visits as (
  SELECT
  DATE_ADD(local_date, INTERVAL row_number DAY) as local_date,
  CAST(visits as FLOAT64) visits,
  fk_sgplaces,
  row_number
FROM
  (
    SELECT
    local_date,
      fk_sgplaces,
      JSON_EXTRACT_ARRAY(visits) AS visit_array, -- Get an array from the JSON string
    FROM
      get_visits
    
  )
CROSS JOIN 
  UNNEST(visit_array) AS visits -- Convert array elements to row
  WITH OFFSET AS row_number -- Get the position in the array as another column
ORDER BY local_date, fk_sgplaces, row_number

),

get_monthly_visits as (
    SELECT fk_sgplaces,  sum(visits) visits, year, month, 
      FROM(
        SELECT *, 
        EXTRACT(MONTH FROM local_date) as month,
        EXTRACT(YEAR FROM local_date) as year
        FROM explode_visits 
      )
      group by fk_sgplaces,year, month
     
),
join_name as (
select b.name, a.*, DATE(CONCAT(year, "-", month, '-01')) as date 
 from get_monthly_visits a
left join
 `{{ var.value.storage_project_id }}.{{ params['places_dataset']  }}.{{ params['places_table'] }}` b
-- `storage-prod-olvin-com.sg_places.20211101` b
on a.fk_sgplaces = b.pid
)

select * from join_name