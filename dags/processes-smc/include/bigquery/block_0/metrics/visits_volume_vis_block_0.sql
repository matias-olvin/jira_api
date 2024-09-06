DELETE `{{ var.value.env_project }}.{{ params['smc_metrics_dataset'] }}.{{ params['visits_volume_vis_table'] }}`
WHERE
    run_date = '{{ ds }}'
AND
    block='0';

INSERT INTO `{{ var.value.env_project }}.{{ params['smc_metrics_dataset'] }}.{{ params['visits_volume_vis_table'] }}`
with ground_truth_table as (
    SELECT fk_sgplaces, visits_per_day AS ground_truth_visits
    FROM
--     `sns-vendor-olvin-poc.smc_gtvm.gtvm_target_agg_sns`
      `{{ params['sns_project'] }}.{{ params['accessible_by_olvin_dataset'] }}.v-{{ params['smc_gtvm_dataset'] }}-{{ params['gtvm_target_agg_sns_table'] }}`
),


almanac_visits as (
    select
        fk_sgplaces,
        sum(visit_score.opening) / 365 as visits_opening,
        sum(visit_score.original) / 365 as visits_original
    from `{{ var.value.env_project }}.{{ params['smc_poi_visits_dataset'] }}.*`
    where local_date >= DATE_SUB(DATE('{{ ds }}'), INTERVAL 365 DAY) and local_date < DATE('{{ ds }}')
    group by fk_sgplaces
),

calculating_ratio as (
    select *,
            visits_opening / nullif(ground_truth_visits, 0) as ratio_opening,
            visits_original / nullif(ground_truth_visits, 0) as ratio_original,
            log(nullif((visits_opening / nullif(ground_truth_visits, 0)), 0 )) as log_ratio_opening,
            log(nullif((visits_original / nullif(ground_truth_visits, 0)), 0 )) as log_ratio_original,
    from ground_truth_table
    inner join almanac_visits using (fk_sgplaces)
    inner join (select pid as fk_sgplaces, region as state from 
    -- `storage-prod-olvin-com.sg_places.20211101`
    `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['places_dynamic_table'] }}`  
    ) using (fk_sgplaces)
),

median_ratio_per_state as (
    select
        distinct state as region,
        PERCENTILE_CONT(ratio_opening, 0.5) OVER(partition by state)  AS median_ratio_opening,
        PERCENTILE_CONT(ratio_original, 0.5) OVER(partition by state)  AS median_ratio_original,
        PERCENTILE_CONT(log_ratio_opening, 0.5) OVER(partition by state)  AS median_log_ratio_opening,
        PERCENTILE_CONT(log_ratio_original, 0.5) OVER(partition by state)  AS median_log_ratio_original,
    from 
        calculating_ratio
)
select *, DATE('{{ ds }}') as run_date from(
select data_.* except(region),
        region as state,
        states.simplified_polygon as polygon,
        "0" AS block
from median_ratio_per_state data_
inner join 
-- `storage-prod-olvin-com.area_geometries.states` states
`{{ var.value.env_project }}.{{ params['area_geometries_dataset'] }}.{{ params['states_table'] }}` states
using (region)) unpivot ((median_ratio,median_log_ratio) FOR visit_score_step in (
(median_ratio_opening,median_log_ratio_opening) as 'opening',
(median_ratio_original,median_log_ratio_original) as 'original')
);
