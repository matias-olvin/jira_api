with visits_by_group as(
    select
        sum(visit_score_steps.opening) as visit_score_opening,
        sum(visit_score_steps.visit_share) as visit_score_visit_share,
        sum(visit_score_steps.daily_estimation) as visit_score_daily_estimation,
        sum(visit_score_steps.gtvm_factor) as visit_score_gtvm_factor,
        visits.naics_code as naics_code,region, local_date,naics.sub_category as naics_name,
        CASE 
            WHEN visits.fk_sgbrands IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS branded
    from `{{ var.value.env_project }}.{{ params['poi_visits_scaled_dataset'] }}.*` visits
    left join 
    `{{ var.value.env_project }}.{{ params['smc_places_dataset'] }}.{{ params['places_data_table'] }}` places
    on visits.fk_sgplaces=places.pid
    left join
    `{{ var.value.env_project }}.{{ params['base_table_dataset'] }}.{{ params['naics_code_subcategories'] }}` naics
    on visits.naics_code=naics.naics_code
    where 
        local_date=DATE_ADD( DATE('{{ ds }}'), INTERVAL -{{ var.value.latency_days_visits }} DAY )
    group by naics_code, region,local_date,branded, naics_name
)

select *, DATE('{{ var.value.smc_start_date }}') as run_date
from visits_by_group