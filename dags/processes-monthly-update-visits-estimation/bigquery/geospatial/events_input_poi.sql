create
or replace table `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['events_holidays_poi_table'] }}` partition by local_date cluster by identifier as
select
    local_date,
    identifier,
    fk_sgplaces
from
    `{{ var.value.env_project }}.{{ params['events_dataset'] }}.{{ params['holidays_table'] }}`                    
    cross join (
        select
            distinct pid as fk_sgplaces
        from
            `{{ var.value.prod_project }}.{{ params['places_postgres_dataset'] }}.{{ params['SGPlaceRaw_table'] }}`
        where
            fk_sgbrands is not null
    )