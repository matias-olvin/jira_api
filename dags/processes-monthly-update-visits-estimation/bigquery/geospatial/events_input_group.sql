create or replace table

`{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['events_holidays_group_table'] }}`
as

select 
 distinct group_id, local_date, identifier
from
`{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['events_holidays_poi_table'] }}`
inner join 
`{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['grouping_id_table'] }}` using (fk_sgplaces)

