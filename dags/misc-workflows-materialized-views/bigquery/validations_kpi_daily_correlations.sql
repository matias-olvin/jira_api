DROP materialized view if exists `{{ var.value.env_project }}.{{ params['validations_kpi_dataset'] }}.{{ params['daily_correlations_mv'] }}`;

create materialized view `{{ var.value.env_project }}.{{ params['validations_kpi_dataset'] }}.{{ params['daily_correlations_mv'] }}` OPTIONS(enable_refresh=False,allow_non_incremental_definition = true, max_staleness = INTERVAL "1" HOUR) as 
select
  distinct
    id,
    run_date,
    PERCENTILE_CONT(correlation[SAFE_OFFSET(0)].poi_correlation, 0.5) over (partition  by id, run_date) as median_correlation_daily
from (
    select *
    from
      `{{ var.value.env_project }}.{{ params['postgres_metrics_dataset'] }}.{{ params['trend_metrics_table'] }}`
    where
      class_id = 'fk_sgbrands'
      and correlation[SAFE_OFFSET(0)].days = 91
      and granularity = 'day'
      and pipeline = 'postgres'
      and step = 'raw'
    )