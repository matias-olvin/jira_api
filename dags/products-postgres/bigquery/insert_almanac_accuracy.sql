insert into
    `{{ params['project'] }}.{{ params['sns_metrics_dataset'] }}.{{ params['almanac_accuracy_table'] }}`
(
    fk_sgplaces,
    fk_sgbrands,
    brand,
    industry,
    top_category,
    sub_category,
    naics_code,
    street_address,
    city,
    region,
    zipcode,
    metric,
    granularity,
    start_date,
    end_date,
    duration,
    update_date,
    result
)

select 
    accuracy.id as fk_sgplaces,
    brands.fk_sgbrands as fk_sgbrands,
    brands.name as brand,
    brands.industry as industry,
    brands.top_category as top_category,
    brands.sub_category as sub_category,
    brands.naics_code as naics_code,
    brands.street_address as street_address,
    brands.city as city,
    brands.region as region,
    brands.postal_code as zipcode,
    accuracy.metric as metric,
    accuracy.granularity as granularity,
    accuracy.start_date	as start_date,
    accuracy.end_date as end_date,
    accuracy.duration as duration,
    accuracy.update_date as update_date,
    accuracy.result as result,
from 
    `{{ params['sns_project'] }}.{{ params['accessible_by_olvin_dataset'] }}.{{ params['almanac_accuracy_table'] }}` accuracy
join 
    `{{ params['project'] }}.{{ params['postgres_dataset'] }}.{{ params['sgplaceraw_table'] }}` brands
    on accuracy.id = brands.pid
where 
    update_date = "{{ ds }}"
