create or replace TABLE
    -- `storage-dev-olvin-com.visits_estimation.quality_output_previous`
    `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['quality_output_previous_table'] }}`
PARTITION BY local_date
CLUSTER BY fk_sgplaces, update_date
as 

with quality_output as (
    select *
    from
        -- `storage-dev-olvin-com.visits_estimation.quality_output`
        `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['quality_output_table'] }}`
)

select *, CAST(DATE_TRUNC(CAST("{{ ds.format('%Y-%m-%d') }}" AS DATE) , MONTH) AS DATE) as update_date
from
    quality_output
inner join (
    select distinct fk_sgplaces
    from quality_output
    group by fk_sgplaces
    having max(local_date) >= DATE_SUB(DATE_ADD( CAST(DATE_TRUNC(CAST("{{ ds.format('%Y-%m-%d') }}" AS DATE) , MONTH) AS DATE), INTERVAL 10 MONTH), INTERVAL 1 DAY)
    ) using (fk_sgplaces)
