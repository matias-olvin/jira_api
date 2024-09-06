DELETE `{{ var.value.env_project }}.{{ params['smc_metrics_dataset'] }}.{{ params['week_categories_distribution_table'] }}`
WHERE run_date = '{{ ds }}';

INSERT INTO `{{ var.value.env_project }}.{{ params['smc_metrics_dataset'] }}.{{ params['week_categories_distribution_table'] }}`
with visits_per_hour_of_week
as
(select
    sum(visit_score_opening) / distinct_category_pois as visit_score_opening_scaled,
    sum(visit_score_visit_share) / distinct_category_pois as visit_score_visit_share_scaled,
    hour_of_week,
    olvin_category
from 
    (
    select 
        *,
        count(distinct fk_sgplaces) over (partition by olvin_category) as distinct_category_pois
    from (
            select fk_sgplaces, (day_of_week-1) * 24 + local_hour as hour_of_week, naics_code,
                visit_score_steps.opening as visit_score_opening,
                visit_score_steps.visit_share as visit_score_visit_share,
                -- NULL as visit_score_geoscaling_global, 
                -- NULL as visit_score_geoscaling_probability
                
            from 
            -- `storage-prod-olvin-com.smc_poi_initial_scaling.2021`
            `{{ var.value.env_project }}.{{ params['poi_visits_block_1_dataset'] }}.2021`
        ) 
    left join (select naics_code, olvin_category from 
    -- `storage-prod-olvin-com.sg_base_tables.naics_code_subcategories`
    `{{ var.value.env_project }}.{{ params['sg_base_tables_dataset'] }}.{{ params['naics_code_subcategories_table'] }}`
    ) using (naics_code)

    )

group by hour_of_week, olvin_category, distinct_category_pois
),
distribution as (
    select
        olvin_category,
        hour_of_week,
        visit_score_opening_scaled / sum(visit_score_opening_scaled) over (partition by hour_of_week) as visit_score_opening_probability,
        visit_score_visit_share_scaled / sum(visit_score_visit_share_scaled) over (partition by hour_of_week) as visit_score_visit_share_probability,
    from visits_per_hour_of_week
)
select *, DATE('{{ ds }}') as run_date
from distribution
left join (
    SELECT olvin_category, TO_JSON_STRING(ARRAY_AGG(sub_category)) as array_sub_categories
    FROM 
    -- `storage-prod-olvin-com.sg_base_tables.naics_code_subcategories`
    `{{ var.value.env_project }}.{{ params['sg_base_tables_dataset'] }}.{{ params['naics_code_subcategories_table'] }}`
    group by olvin_category
)
using(olvin_category);