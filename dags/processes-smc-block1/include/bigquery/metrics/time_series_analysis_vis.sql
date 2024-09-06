DELETE `{{ var.value.env_project }}.{{ params['smc_metrics_dataset'] }}.{{ params['time_series_analysis_vis_block_1_table'] }}`
WHERE run_date = '{{ ds }}';

INSERT INTO `{{ var.value.env_project }}.{{ params['smc_metrics_dataset'] }}.{{ params['time_series_analysis_vis_block_1_table'] }}`
with visits_by_group as(
    select
        sum(visit_score_steps.opening) as visit_score_opening,
        sum(visit_score_steps.visit_share) as visit_score_visit_share,
        visits.naics_code as naics_code,region, local_date,naics.sub_category as naics_name,
        CASE 
            WHEN visits.fk_sgbrands IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS branded
    from `{{ var.value.env_project }}.{{ params['poi_visits_block_1_dataset'] }}.*` visits
    left join 
    `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['places_dynamic_table'] }}` places
    on visits.fk_sgplaces=places.pid
    left join
    `{{ var.value.env_project }}.{{ params['sg_base_tables_dataset'] }}.{{ params['naics_code_subcategories_table'] }}` naics
    on visits.naics_code=naics.naics_code
    group by naics_code, region,local_date,branded, naics_name
)
select *, DATE('{{ ds }}') as run_date
from visits_by_group;