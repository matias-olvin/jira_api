DELETE `{{ var.value.env_project }}.{{ params['smc_metrics_dataset'] }}.{{ params['standalone_vs_child_metrics_table'] }}`
WHERE
    run_date = '{{ ds }}'
AND
    block='daily_estimation';

INSERT INTO `{{ var.value.env_project }}.{{ params['smc_metrics_dataset'] }}.{{ params['standalone_vs_child_metrics_table'] }}`
with calculating_ratio_standalone_child as

    (select *, median_historical_visits_standalone / nullif(median_historical_visits_child,0) as ratio_standalone_child
    from  (
            SELECT fk_sgbrands, name, block, visit_score_step, median_historical_visits as median_historical_visits_standalone,
            run_date 
            FROM  `{{ var.value.env_project }}.{{ params['smc_metrics_dataset'] }}.{{ params['standalone_vs_child_vis_table'] }}`
            where standalone_bool = true and run_date = DATE('{{ ds }}') and block = 'daily_estimation'
    )
    inner join (
            SELECT fk_sgbrands, name, block, visit_score_step, median_historical_visits as median_historical_visits_child
            FROM  `{{ var.value.env_project }}.{{ params['smc_metrics_dataset'] }}.{{ params['standalone_vs_child_vis_table'] }}`
            where standalone_bool = false
        )
    using(fk_sgbrands, name, block, visit_score_step)
    )
select 
    distinct
    block, visit_score_step, run_date,
    avg(ratio_standalone_child)  over (partition by block, visit_score_step) as avg_ratio_standalone_child,
    PERCENTILE_CONT(ratio_standalone_child, 0.5) OVER(partition by block, visit_score_step)  AS median_ratio_standalone_child
from calculating_ratio_standalone_child;