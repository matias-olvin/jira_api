DELETE `{{ var.value.env_project }}.{{ params['smc_metrics_dataset'] }}.{{ params['visits_volume_vis_table'] }}`
WHERE
    run_date = '{{ ds }}'
AND
    block='groundtruth';

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
        sum(visit_score) / 365 as visits_score_gtvm,
    from `{{ var.value.env_project }}.{{ params['smc_poi_visits_scaled_dataset'] }}.*`
    where local_date >= DATE_SUB(DATE('{{ ds }}'), INTERVAL 365 DAY) and local_date < DATE('{{ ds }}')
    group by fk_sgplaces
),

calculating_ratio as (
    select *,
            visits_score_gtvm / nullif(ground_truth_visits, 0) as ratio_visits_score_gtvm,
            log(nullif((visits_score_gtvm / nullif(ground_truth_visits, 0)), 0 )) as log_ratio_visits_score_gtvm,
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
        PERCENTILE_CONT(ratio_visits_score_gtvm, 0.5) OVER(partition by state)  AS median_ratio_visits_score_gtvm,
        PERCENTILE_CONT(log_ratio_visits_score_gtvm, 0.5) OVER(partition by state)  AS median_log_ratio_visits_score_gtvm,
        from 
        calculating_ratio
)
select *, DATE('{{ ds }}') as run_date from(
select data_.* except(region),
        region as state,
        states.simplified_polygon as polygon,
        "groundtruth" AS block
from median_ratio_per_state data_
inner join 
-- `storage-prod-olvin-com.area_geometries.states` states
`{{ var.value.env_project }}.{{ params['area_geometries_dataset'] }}.{{ params['states_table'] }}` states
using (region)) unpivot ((median_ratio,median_log_ratio) FOR visit_score_step in (
(median_ratio_visits_score_gtvm,median_log_ratio_visits_score_gtvm) as 'GTVM/Final')
);
