create
or replace table `storage-dev-olvin-com.visits_estimation_model_dev.events_holidays` partition by local_date cluster by identifier as
select
    local_date,
    identifier,
    fk_sgplaces
from
    `storage-prod-olvin-com.events.holidays`                    
    cross join (
        select
            distinct pid as fk_sgplaces
        from
            `storage-prod-olvin-com.postgres_batch.SGPlaceRaw`
        where
            fk_sgbrands is not null
    )